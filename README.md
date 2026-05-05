# Consolidador de Incidentes (Netlify + Supabase)

Aplicación fullstack JavaScript para:

- Login con roles `admin` y `user`
- Login por correo y contraseña
- Administración de usuarios (solo admin)
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

## Deploy en Netlify

1. Sube este repositorio a GitHub.
2. Crea un sitio en Netlify conectado al repo.
3. Define variables de entorno del `.env` en Netlify.
4. Deploy.

Netlify detectará `netlify.toml` y publicará frontend + functions.
