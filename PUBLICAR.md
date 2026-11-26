# Cómo publicar en Espacio Digital

Cada publicación es **un solo archivo Markdown** (`.md`) en la carpeta
[`src/content/publicaciones/`](src/content/publicaciones/). Al subirlo a GitHub,
Vercel actualiza la web sola en alrededor de un minuto.

- La portada muestra las **6 más recientes**; todas están en `/publicaciones`.
- El **nombre del archivo** es la dirección: `dolar-vs-merval.md` → `/publicaciones/dolar-vs-merval`.
  Usá minúsculas, sin espacios ni tildes, con guiones entre palabras.

## Los campos del encabezado

Todo archivo empieza con un bloque entre `---`:

```yaml
---
titulo: "Dólar vs. Merval en 2026"
fecha: 2026-11-26          # año-mes-día
categoria: finanzas        # finanzas | programacion | ciencia | notas
resumen: "Una o dos frases para la tarjeta de la portada."
link: "https://..."        # opcional: para compartir algo externo
borrador: false            # opcional: true = no se publica
---
```

Debajo va el texto en Markdown. La plantilla
[`_plantilla.md`](src/content/publicaciones/_plantilla.md) tiene ejemplos de títulos,
listas, tablas, código e imágenes (los archivos que empiezan con `_` no se publican).

---

## Opción A: desde el navegador (GitHub)

1. Entrá a <https://github.com/montesquieu891/espacio-digital/tree/main/src/content/publicaciones>.
2. Abrí `_plantilla.md`, tocá el ícono **Copy raw file** (dos cuadraditos) y volvé a la carpeta.
3. **Add file → Create new file**. Escribí el nombre (por ejemplo `dolar-vs-merval.md`),
   pegá la plantilla y completala. Poné `borrador: false`.
4. **Commit changes…** → **Commit changes**.
5. En un minuto está en la web.

**Imágenes:** en la misma carpeta, **Add file → Upload files**, subí la imagen y en el
texto escribí `![Descripción](./nombre-de-la-imagen.png)`.

**Editar o despublicar:** abrí el archivo, tocá el lápiz ✏️, cambiá lo que quieras
(o poné `borrador: true` para ocultarlo) y hacé commit.

## Opción B: desde tu computadora

En una terminal, dentro de la carpeta del proyecto:

```powershell
git pull
```

> Siempre primero `git pull`: el robot de mercados sube datos todos los días y así
> evitás conflictos.

1. Copiá `src/content/publicaciones/_plantilla.md` con el nombre nuevo y completalo.
2. Para verla antes de publicar:

   ```powershell
   npm run dev
   ```

   y abrí <http://localhost:4321>. Acá **sí se ven los borradores** (con una etiqueta
   amarilla), así podés revisarlos. Cortá con `Ctrl + C`.
3. Para publicar:

   ```powershell
   git add src/content/publicaciones
   git commit -m "Nueva publicación: dolar-vs-merval"
   git push
   ```

---

## Si algo sale mal

- Si un dato del encabezado está mal (por ejemplo, una categoría que no existe o una
  fecha inválida), **la web no se rompe**: Vercel no publica el cambio y la versión
  anterior sigue en línea.
- Para ver el error: en GitHub, al lado del último commit aparece una **✗ roja** →
  **Details**. El mensaje dice qué archivo y qué campo revisar. En tu computadora,
  `npm run dev` muestra el mismo error.
- Para agregar o renombrar categorías, editá la lista `CATEGORIAS` en
  [`src/content.config.ts`](src/content.config.ts) y los nombres visibles en
  [`src/core/publicaciones.ts`](src/core/publicaciones.ts).
