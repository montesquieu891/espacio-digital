import yfinance as yf
import pandas as pd
import json
import os
import sys
import tempfile
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from pathlib import Path
import numpy as np

# Configuración
# Nota: el ticker de YPF en BYMA para Yahoo Finance es YPFD.BA (YPF.BA no existe).
PORTFOLIO = ["SPY", "MELI", "AAPL", "GOOGL", "TSLA", "BTC-USD", "GGAL.BA", "YPFD.BA"]
OUTPUT_PATH = Path(__file__).parent.parent / "public" / "market_data.json"
TIMEZONE = ZoneInfo("America/Argentina/Buenos_Aires")

def calculate_technical_indicators(df):
    """Calcula indicadores técnicos básicos"""
    # SMA (Simple Moving Average) 20 y 50 días
    df['SMA_20'] = df['Close'].rolling(window=20).mean()
    df['SMA_50'] = df['Close'].rolling(window=50).mean()
    
    # Bollinger Bands (20 días, 2 std)
    df['BB_middle'] = df['Close'].rolling(window=20).mean()
    df['BB_std'] = df['Close'].rolling(window=20).std()
    df['BB_upper'] = df['BB_middle'] + (df['BB_std'] * 2)
    df['BB_lower'] = df['BB_middle'] - (df['BB_std'] * 2)
    
    # RSI (Relative Strength Index) 14 días
    delta = df['Close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    df['RSI'] = 100 - (100 / (1 + rs))
    
    # MACD
    ema_12 = df['Close'].ewm(span=12, adjust=False).mean()
    ema_26 = df['Close'].ewm(span=26, adjust=False).mean()
    df['MACD'] = ema_12 - ema_26
    df['MACD_signal'] = df['MACD'].ewm(span=9, adjust=False).mean()
    
    return df

def analyze_performance(df):
    """Análisis de performance y estadísticas"""
    current_price = df['Close'].iloc[-1]
    
    # Calcular retornos en diferentes períodos
    periods = {
        '1D': 1,
        '5D': 5,
        '1M': 21,  # ~1 mes de trading
        '3M': 63,  # ~3 meses
        '6M': 126, # ~6 meses
        '1Y': 252  # ~1 año
    }
    
    returns = {}
    for label, days in periods.items():
        if len(df) > days:
            past_price = df['Close'].iloc[-days-1]
            returns[label] = round(((current_price - past_price) / past_price) * 100, 2)
        else:
            returns[label] = None
    
    # Volatilidad (desviación estándar anualizada)
    daily_returns = df['Close'].pct_change().dropna()
    volatility = round(daily_returns.std() * np.sqrt(252) * 100, 2)
    
    # Máximo y mínimo del período
    high_52w = df['High'].max()
    low_52w = df['Low'].min()
    
    # Distancia desde máximo/mínimo
    distance_from_high = round(((current_price - high_52w) / high_52w) * 100, 2)
    distance_from_low = round(((current_price - low_52w) / low_52w) * 100, 2)
    
    return {
        "returns": returns,
        "volatility": volatility,
        "high_52w": round(high_52w, 2),
        "low_52w": round(low_52w, 2),
        "distance_from_high": distance_from_high,
        "distance_from_low": distance_from_low,
        "avg_volume_30d": int(df['Volume'].tail(30).mean())
    }

def fetch_historical_data(symbol, period="1y"):
    """Descarga y procesa datos históricos con indicadores técnicos"""
    try:
        ticker = yf.Ticker(symbol)
        
        # Descargar datos históricos
        hist = ticker.history(period=period)
        
        if hist.empty:
            print(f"⚠️  No hay datos para {symbol}")
            return None

        # Descartar días sin precio de cierre (Yahoo a veces devuelve la fila del día
        # en curso, o feriados, con Close = NaN). Si no se descartan, el NaN se propaga
        # a los rendimientos y a la volatilidad, y termina escrito en el JSON.
        filas_antes = len(hist)
        hist = hist.dropna(subset=['Close'])
        descartadas = filas_antes - len(hist)
        if descartadas:
            print(f"   ↳ {symbol}: descartadas {descartadas} fila(s) sin precio de cierre")

        if hist.empty:
            print(f"⚠️  {symbol}: ninguna fila tiene precio de cierre")
            return None

        # Calcular indicadores técnicos
        hist = calculate_technical_indicators(hist)
        
        # Análisis de performance
        performance = analyze_performance(hist)
        
        # Preparar datos para el frontend (últimos 365 días)
        hist_clean = hist.tail(365).copy()
        
        # Convertir a formato JSON-friendly
        historical_data = {
            "dates": hist_clean.index.strftime('%Y-%m-%d').tolist(),
            "prices": {
                "close": hist_clean['Close'].round(2).fillna(0).tolist(),
                "open": hist_clean['Open'].round(2).fillna(0).tolist(),
                "high": hist_clean['High'].round(2).fillna(0).tolist(),
                "low": hist_clean['Low'].round(2).fillna(0).tolist(),
            },
            "volume": hist_clean['Volume'].fillna(0).astype(int).tolist(),
            "indicators": {
                "sma_20": hist_clean['SMA_20'].round(2).fillna(0).tolist(),
                "sma_50": hist_clean['SMA_50'].round(2).fillna(0).tolist(),
                "bb_upper": hist_clean['BB_upper'].round(2).fillna(0).tolist(),
                "bb_middle": hist_clean['BB_middle'].round(2).fillna(0).tolist(),
                "bb_lower": hist_clean['BB_lower'].round(2).fillna(0).tolist(),
                "rsi": hist_clean['RSI'].round(2).fillna(50).tolist(),
                "macd": hist_clean['MACD'].round(2).fillna(0).tolist(),
                "macd_signal": hist_clean['MACD_signal'].round(2).fillna(0).tolist(),
            },
            "performance": performance
        }
        
        return historical_data
        
    except Exception as e:
        print(f"❌ Error procesando {symbol}: {e}")
        return None

def fetch_current_data(symbol):
    """Obtiene datos actuales del ticker"""
    try:
        ticker = yf.Ticker(symbol)
        info = ticker.info
        
        # Datos actuales
        current_price = info.get('currentPrice') or info.get('regularMarketPrice', 0)
        previous_close = info.get('previousClose', current_price)
        change = current_price - previous_close
        change_percent = (change / previous_close * 100) if previous_close else 0
        
        # Noticias (últimas 3)
        news_raw = ticker.news if hasattr(ticker, 'news') else []
        news = []
        for item in news_raw:
            if len(news) >= 3: break
            
            # Validar campos obligatorios y buscar alternativas para el link
            title = item.get('title')
            link = item.get('link') or item.get('url')
            
            if title and link:
                news.append({
                    "title": title,
                    "url": link,
                    "published": datetime.fromtimestamp(
                        item.get('providerPublishTime', datetime.now().timestamp()),
                        tz=TIMEZONE
                    ).isoformat(),
                    "source": item.get('publisher', 'Unknown')
                })
        
        return {
            "symbol": symbol,
            "name": info.get('longName', symbol),
            "updated_at": datetime.now(TIMEZONE).strftime("%d/%m/%Y %H:%M"),
            "price": round(current_price, 2),
            "change": round(change, 2),
            "change_percent": round(change_percent, 2),
            "sector": info.get('sector', 'N/A'),
            "industry": info.get('industry', 'N/A'),
            "description": info.get('longBusinessSummary', '')[:300] + '...',
            "metrics": {
                "pe_ratio": round(info.get('trailingPE', 0), 2) if info.get('trailingPE') else None,
                "market_cap": info.get('marketCap', 0),
                "volume": info.get('volume', 0),
                "avg_volume": info.get('averageVolume', 0),
                "dividend_yield": round(info.get('dividendYield', 0) * 100, 2) if info.get('dividendYield') else None,
                "beta": round(info.get('beta', 0), 2) if info.get('beta') else None,
            },
            "news": news
        }
        
    except Exception as e:
        print(f"❌ Error obteniendo datos de {symbol}: {e}")
        return None

def is_json_safe(obj):
    """True si el objeto se puede escribir como JSON válido (sin NaN ni Infinity)."""
    try:
        json.dumps(obj, allow_nan=False)
        return True
    except (ValueError, TypeError):
        return False


def load_previous_assets(path):
    """Lee el JSON publicado anteriormente y devuelve {símbolo: datos}.
    Solo se aceptan activos válidos, para no arrastrar datos rotos."""
    try:
        with open(path, encoding='utf-8') as f:
            data = json.load(f)
    except (OSError, ValueError):
        return {}
    return {
        a["symbol"]: a
        for a in data.get("assets", [])
        if isinstance(a, dict) and "symbol" in a and is_json_safe(a)
    }


def warn(message):
    """Imprime un aviso; en GitHub Actions aparece resaltado en el resumen."""
    prefix = "::warning::" if os.environ.get("GITHUB_ACTIONS") else "⚠️  "
    print(f"{prefix}{message}")


def save_json_atomically(data, path):
    """Escribe el JSON en un archivo temporal y, solo si salió bien, reemplaza
    al anterior. Si algo falla, el archivo publicado queda intacto."""
    path.parent.mkdir(exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(dir=path.parent, prefix=".tmp_", suffix=".json")
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as f:
            # allow_nan=False: si se colara un NaN, falla acá en vez de publicar JSON inválido
            json.dump(data, f, indent=2, ensure_ascii=False, allow_nan=False)
        os.replace(tmp_path, path)
    except BaseException:
        os.unlink(tmp_path)
        raise


def main():
    """Función principal"""
    print("🚀 Iniciando actualización de datos de mercado...")

    previous = load_previous_assets(OUTPUT_PATH)
    assets = []
    reused = []

    for symbol in PORTFOLIO:
        print(f"📊 Procesando {symbol}...")

        asset_data = None
        current_data = fetch_current_data(symbol)
        historical_data = fetch_historical_data(symbol, period="1y") if current_data else None

        if current_data and historical_data:
            candidate = {**current_data, "historical": historical_data}
            if is_json_safe(candidate):
                asset_data = candidate
            else:
                warn(f"{symbol}: los datos nuevos contienen valores inválidos (NaN/Infinity)")

        if asset_data is None:
            if symbol in previous:
                warn(f"{symbol}: se conserva el dato anterior ({previous[symbol].get('updated_at', '?')})")
                asset_data = previous[symbol]
                reused.append(symbol)
            else:
                warn(f"{symbol}: sin datos nuevos ni anteriores; se omite")
                continue

        assets.append(asset_data)

    nuevos = len(assets) - len(reused)
    if nuevos == 0:
        # Si no llegó ni un dato nuevo (p. ej. Yahoo caído), no se reescribe el archivo:
        # se evita ponerle fecha de hoy a datos viejos y el workflow queda marcado en rojo.
        print("❌ No se obtuvo ningún dato nuevo. No se modifica el archivo.")
        sys.exit(1)

    # Metadata
    output = {
        "meta": {
            "last_update": datetime.now(TIMEZONE).isoformat(),
            "version": "2.0",
            "assets_count": len(assets)
        },
        "assets": assets
    }

    save_json_atomically(output, OUTPUT_PATH)

    print(f"✅ Datos actualizados: {nuevos} activos nuevos, {len(reused)} conservados del día anterior")
    print(f"📁 Archivo guardado en: {OUTPUT_PATH}")

if __name__ == "__main__":
    main()