// @ts-check
import { defineConfig } from 'astro/config';

import tailwindcss from '@tailwindcss/vite';

// https://astro.build/config
export default defineConfig({
  // Astro 7 cambió el valor por defecto a 'jsx', que puede borrar espacios entre
  // elementos en línea (p. ej. "Finanzas · 26 nov"). true mantiene el HTML como antes.
  compressHTML: true,
  vite: {
    plugins: [tailwindcss()]
  }
});