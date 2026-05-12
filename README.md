# Consolidador de Incidentes (Netlify + Supabase)

Aplicación fullstack JavaScript para:

- Login con roles `admin` y `user`
- Login por correo y contraseña
- Administración de usuarios (solo admin)
- Administración de áreas y tags por área (solo admin)
- Registro de incidentes entrantes por webhook
- Enlace automático incidente -> usuario por número telefónico
- Visualización en tiempo real de incidentes en el dashboard

## Arquitectura

- Frontend estático en `public/`
- Backend serverless en `netlify/functions/`
- Base de datos y realtime en Supabase

## Variables de entorno

Copiar `.env.example` a `.env` y completar:

- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY`
- `SUPABASE_ANON_KEY`
- `JWT_SECRET`
- `WEBHOOK_SHARED_SECRET` (opcional, recomendado)

## Configuración inicial

1. Instala dependencias:

```bash
npm install
```

2. En Supabase, ejecuta el script:

- `sql/schema.sql`

Credenciales iniciales luego del script:

- Correo: `admin@notificaciones.local`
- Contraseña: `Admin123!`

3. En Supabase habilita Realtime para la tabla `incidents`:

- Database -> Replication -> activar tabla `incidents`

4. Levanta el proyecto local:

```bash
npm run dev
```

## Endpoints

- `POST /api/auth-login` login
- `GET /api/users-list` lista usuarios (admin)
- `POST /api/users-create` crear usuarios (admin)
- `POST /api/users-update` editar usuarios (admin)
- `POST /api/users-delete` eliminar usuarios (admin)
- `GET /api/areas-list` lista áreas
- `POST /api/areas-create` crear área (admin)
- `POST /api/areas-update` editar área (admin)
- `POST /api/areas-delete` eliminar área (admin)
- `GET /api/incidents-list` lista incidentes
- `POST /api/incidents-delete` eliminar incidentes (admin)
- `POST /api/webhook-incident` webhook de incidentes
- `GET /api/public-config` config pública Supabase para frontend

## Payload sugerido para webhook

```json
{
  "incident_title": "Caida monitor checkout",
  "incident_status": "OPEN",
  "incident_severity": "CRITICAL",
  "incident_description": "El endpoint /checkout responde 5xx",
  "called_number": "+56912345678"
}
```

Si `called_number` coincide con `phone_normalized` de un usuario, el incidente queda enlazado automáticamente.

## Integración con tu Twilio Function actual

Desde tu lógica actual en `twiliofunction.js`, agrega un POST hacia:

- `/api/webhook-incident` en local o
- `https://TU-SITIO.netlify.app/api/webhook-incident` en producción

Con eso, cuando llegue una alerta, el panel la mostrará de inmediato.

### Variables en Twilio Function (Console > Functions > Environment Variables)

- `APP_WEBHOOK_URL`: URL del webhook del aplicativo. Ejemplo `https://TU-SITIO.netlify.app/api/webhook-incident`
- `APP_WEBHOOK_SECRET`: mismo secreto configurado en Netlify como `WEBHOOK_SHARED_SECRET` (opcional, recomendado)
- `AREA_TAG_GROUPS_JSON`: JSON opcional para definir tags por área (si no se define, usa defaults de `twiliofunction.js`)
- `ONCALL_CONTACTS`: JSON opcional para nombre por número, por ejemplo:

```json
{
  "+56912345678": "Ana Torres",
  "+56987654321": "Diego Perez"
}
```

Flujo esperado:

1. Twilio Function recibe evento y determina incidente crítico.
2. Dispara canales base (teléfono, SMS y Teams).
3. En paralelo envía POST al aplicativo con el incidente y `called_number`.
4. El aplicativo enlaza por número con `app_users` y muestra el usuario llamado en tiempo real.

## Flujo de áreas y tags

1. `twiliofunction.js` detecta el área del incidente según `entityTags` (usando `AREA_TAG_GROUPS_JSON` o defaults internos).
2. La selección del especialista que se llama la resuelve `ONCALL_ROSTER`.
3. El panel web mantiene foco en administración de usuarios (no en edición de tags/áreas).
4. El webhook persiste `incident_area` y prioriza vincular usuario por número dentro de esa área cuando el área está disponible.

Ejemplo de `AREA_TAG_GROUPS_JSON`:

```json
{
  "Area SRE": [
    "custom_call_turno_observabilidad",
    "custom:call_turno_observabilidad",
    "call_turno_observabilidad"
  ],
  "Area Programacion": [
    "custom_programacion",
    "custom:_programacion",
    "call_turno_progra"
  ]
}
```

Ejemplo recomendado de `ONCALL_ROSTER` por área:

```json
{
  "default": {
    "level1": ["+56911111111"],
    "level2": ["+56922222222"]
  },
  "areas": {
    "AREA_SRE": {
      "level1": ["+56933333333"],
      "level2": ["+56944444444"]
    },
    "AREA_PROGRAMACION": {
      "level1": ["+56955555555"],
      "level2": ["+56966666666"]
    }
  }
}
```

Notas:

- También sigue soportado el formato legacy: `{ "level1": [...], "level2": [...] }`.
- Si llega `payload.level1` o `payload.level2`, eso tiene prioridad sobre `ONCALL_ROSTER`.

## Deploy en Netlify

1. Sube este repositorio a GitHub.
2. Crea un sitio en Netlify conectado al repo.
3. Define variables de entorno del `.env` en Netlify.
4. Deploy.

Netlify detectará `netlify.toml` y publicará frontend + functions.
