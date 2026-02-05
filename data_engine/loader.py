import yfinance as yf
import json
from pathlib import Path
from datetime import datetime

PORTFOLIO = ["SPY", "MELI", "BTC-USD", "NVDA"]

class MarketEngine:
    def __init__(self):
        # Apunta a la carpeta public de la web
        self.output_path = Path("../public/market_data.json")
    
    def fetch_data(self):
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Iniciando secuencia de descarga...")
        results = []
        
        for ticker in PORTFOLIO:
            try:
                stock = yf.Ticker(ticker)
                hist = stock.history(period="5d")
                
                if hist.empty:
                    print(f"⚠ Error: No data for {ticker}")
                    continue
                
                current_price = hist['Close'].iloc[-1]
                prev_price = hist['Close'].iloc[-2]
                change_pct = ((current_price - prev_price) / prev_price) * 100
                
                data_point = {
                    "symbol": ticker,
                    "price": round(current_price, 2),
                    "change_percent": round(change_pct, 2),
                    "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M")
                }
                
                results.append(data_point)
                print(f"✓ {ticker}: ${current_price:.2f} ({change_pct:+.2f}%)")
                
            except Exception as e:
                print(f"X Error procesando {ticker}: {e}")
                
        return results

    def save_to_web(self, data):
        # Crea la carpeta public si no existe (por seguridad)
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.output_path, 'w') as f:
            json.dump(data, f, indent=2)
        print(f"\n[ÉXITO] Datos inyectados en {self.output_path}")

if __name__ == "__main__":
    engine = MarketEngine()
    data = engine.fetch_data()
    engine.save_to_web(data)