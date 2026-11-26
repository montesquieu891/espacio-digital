"""
SGD with Momentum Optimization Engine (Adapted for Astro)
Based on code provided by user (Claude generated).
"""

import numpy as np
import json
from pathlib import Path
from typing import Tuple, Dict, List, Callable

# --- CONFIGURACIÓN DE RUTAS (Agregado para Espacio Digital) ---
BASE_DIR = Path(__file__).parent.parent
OUTPUT_PATH = BASE_DIR / "public" / "optimization_data.json"

class LossSurface:
    """Non-convex loss functions with multiple local minima"""
    
    @staticmethod
    def rastrigin(x: np.ndarray, y: np.ndarray, A: float = 10) -> np.ndarray:
        return (A * 2 + (x**2 - A * np.cos(2 * np.pi * x)) + 
                (y**2 - A * np.cos(2 * np.pi * y)))
    
    @staticmethod
    def himmelblau(x: np.ndarray, y: np.ndarray) -> np.ndarray:
        return (x**2 + y - 11)**2 + (x + y**2 - 7)**2
    
    @staticmethod
    def ackley(x: np.ndarray, y: np.ndarray, a: float = 20, b: float = 0.2, c: float = 2*np.pi) -> np.ndarray:
        term1 = -a * np.exp(-b * np.sqrt(0.5 * (x**2 + y**2)))
        term2 = -np.exp(0.5 * (np.cos(c * x) + np.cos(c * y)))
        return term1 + term2 + a + np.e
    
    @staticmethod
    def rosenbrock(x: np.ndarray, y: np.ndarray, a: float = 1, b: float = 100) -> np.ndarray:
        return (a - x)**2 + b * (y - x**2)**2

def generate_surface_data(loss_function: str, x_range: Tuple[float, float], y_range: Tuple[float, float], resolution: int = 60) -> Dict:
    """Generate 3D surface mesh data for visualization"""
    x = np.linspace(x_range[0], x_range[1], resolution)
    y = np.linspace(y_range[0], y_range[1], resolution)
    X, Y = np.meshgrid(x, y)
    
    loss_functions = {
        'rastrigin': LossSurface.rastrigin,
        'himmelblau': LossSurface.himmelblau,
        'ackley': LossSurface.ackley,
        'rosenbrock': LossSurface.rosenbrock
    }
    
    if loss_function not in loss_functions:
        raise ValueError(f"Unknown loss function: {loss_function}")
    
    loss_fn = loss_functions[loss_function]
    Z = loss_fn(X, Y)
    
    # Normalización visual para funciones con picos extremos (como Rosenbrock)
    if loss_function == 'rosenbrock':
        Z = np.log(Z + 1)

    return {
        'x': x.tolist(),
        'y': y.tolist(),
        'z': Z.tolist(),
        'bounds': {'x': x_range, 'y': y_range}
    }

def main():
    print("🧪  Generando Atlas de Optimización (Claude Engine)...")
    
    # Generamos el paquete completo de mapas
    data_pack = {
        "rastrigin": generate_surface_data('rastrigin', (-5.12, 5.12), (-5.12, 5.12)),
        "himmelblau": generate_surface_data('himmelblau', (-5, 5), (-5, 5)),
        "ackley": generate_surface_data('ackley', (-5, 5), (-5, 5)),
        "rosenbrock": generate_surface_data('rosenbrock', (-2, 2), (-2, 2))
    }
    
    # Guardamos en la carpeta public de Astro
    OUTPUT_PATH.parent.mkdir(exist_ok=True)
    with open(OUTPUT_PATH, 'w') as f:
        json.dump(data_pack, f)
        
    print(f"✅ Datos guardados exitosamente en: {OUTPUT_PATH}")

if __name__ == '__main__':
    main()