// Funciones compartidas para leer y mostrar publicaciones.
import { getCollection, type CollectionEntry } from "astro:content";

export type Publicacion = CollectionEntry<"publicaciones">;

/**
 * Publicaciones ordenadas de la más nueva a la más vieja.
 * Los borradores solo se incluyen en desarrollo (`npm run dev`), nunca en la web publicada.
 */
export async function getPublicaciones(): Promise<Publicacion[]> {
  const todas = await getCollection(
    "publicaciones",
    ({ data }) => import.meta.env.DEV || !data.borrador,
  );
  return todas.sort(
    (a, b) => b.data.fecha.getTime() - a.data.fecha.getTime() || a.id.localeCompare(b.id),
  );
}

/**
 * Fecha legible, p. ej. "26 nov 2026".
 * Se usa UTC porque una fecha escrita como 2026-11-26 se interpreta a las 00:00 UTC;
 * con la hora de Argentina se mostraría como el día anterior.
 */
export function formatearFecha(fecha: Date): string {
  const partes = new Intl.DateTimeFormat("es-AR", {
    day: "numeric",
    month: "short",
    year: "numeric",
    timeZone: "UTC",
  }).formatToParts(fecha);
  const parte = (tipo: string) => partes.find((p) => p.type === tipo)?.value ?? "";
  return `${parte("day")} ${parte("month").replace(".", "")} ${parte("year")}`;
}

export const NOMBRE_CATEGORIA: Record<Publicacion["data"]["categoria"], string> = {
  finanzas: "Finanzas",
  programacion: "Programación",
  ciencia: "Ciencia",
  notas: "Notas",
};
