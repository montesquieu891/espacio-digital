// Colecciones de contenido de Astro.
// Cada archivo .md dentro de src/content/publicaciones/ es una publicación.
// El nombre del archivo define la dirección: mi-analisis.md -> /publicaciones/mi-analisis
// Los archivos que empiezan con "_" (como _plantilla.md) se ignoran.
import { defineCollection } from "astro:content";
import { glob } from "astro/loaders";
import { z } from "astro/zod";

// Para agregar o renombrar una categoría, editá esta lista.
export const CATEGORIAS = ["finanzas", "programacion", "ciencia", "notas"] as const;

// Un campo opcional que quedó vacío (link: "" o borrador: sin valor, como lo puede
// guardar un editor visual) se trata como si no estuviera.
const vacioComoAusente = (v: unknown) => (v === "" || v === null ? undefined : v);

const publicaciones = defineCollection({
  loader: glob({
    base: "./src/content/publicaciones",
    pattern: ["**/*.md", "!**/_*.md"],
  }),
  schema: z.object({
    titulo: z.string().min(1, "Falta el título"),
    fecha: z.coerce.date(),
    categoria: z.enum(CATEGORIAS, {
      error: `La categoría debe ser una de: ${CATEGORIAS.join(", ")}`,
    }),
    resumen: z.string().min(1, "Falta el resumen"),
    // Para compartir algo externo (un artículo, un paper, un video...)
    link: z.preprocess(
      vacioComoAusente,
      z.url({ error: "El link debe ser una dirección completa (https://...)" }).optional(),
    ),
    // borrador: true -> no se publica (solo se ve con `npm run dev`)
    borrador: z.preprocess(vacioComoAusente, z.boolean().default(false)),
  }),
});

export const collections = { publicaciones };
