-- Ejecutar en Supabase SQL Editor
create extension if not exists pgcrypto;

create table if not exists public.app_users (
  id uuid primary key default gen_random_uuid(),
  username text not null unique,
  email text not null,
  password_hash text not null,
  phone text not null,
  phone_normalized text not null,
  role text not null default 'user' check (role in ('user', 'admin')),
  is_active boolean not null default true,
  created_at timestamptz not null default now()
);

alter table public.app_users add column if not exists email text;
update public.app_users
set email = lower(username || '@local.invalid')
where email is null or btrim(email) = '';
alter table public.app_users alter column email set not null;

create index if not exists app_users_phone_normalized_idx on public.app_users(phone_normalized);
create unique index if not exists app_users_email_unique_idx on public.app_users(lower(email));

create table if not exists public.incidents (
  id bigserial primary key,
  problem_id text,
  incident_title text not null,
  incident_status text not null default 'OPEN',
  incident_severity text not null default 'UNKNOWN',
  incident_description text,
  called_number text,
  called_user_id uuid references public.app_users(id) on delete set null,
  called_user_name text,
  incident_attended boolean not null default false,
  incident_attended_at timestamptz,
  created_at timestamptz not null default now()
);

alter table public.incidents add column if not exists problem_id text;
alter table public.incidents add column if not exists incident_attended boolean;
alter table public.incidents add column if not exists incident_attended_at timestamptz;
update public.incidents set incident_attended = false where incident_attended is null;
alter table public.incidents alter column incident_attended set default false;
alter table public.incidents alter column incident_attended set not null;

create index if not exists incidents_created_at_idx on public.incidents(created_at desc);
create index if not exists incidents_called_number_idx on public.incidents(called_number);
create index if not exists incidents_problem_id_idx on public.incidents(problem_id);

alter table public.app_users enable row level security;
alter table public.incidents enable row level security;

-- app_users solo por backend (service role)
drop policy if exists app_users_no_access on public.app_users;
create policy app_users_no_access on public.app_users
for all
using (false)
with check (false);

-- incidentes visibles para cliente realtime (anon/authenticated)
drop policy if exists incidents_read_anon on public.incidents;
create policy incidents_read_anon on public.incidents
for select
to anon
using (true);

drop policy if exists incidents_read_authenticated on public.incidents;
create policy incidents_read_authenticated on public.incidents
for select
to authenticated
using (true);

-- Usuario admin inicial (cambiar credenciales antes de producción)
-- Password inicial: Admin123!
insert into public.app_users (username, email, password_hash, phone, phone_normalized, role, is_active)
values (
  'admin',
  'admin@notificaciones.local',
  '$2a$10$aTgCnE/CNKCDAL/i78vDoeVltFM85ZTgi4RP98TtBQw1qv.EP711.',
  '+56900000000',
  '56900000000',
  'admin',
  true
)
on conflict (username) do nothing;

update public.app_users
set email = 'admin@notificaciones.local'
where username = 'admin' and (email is null or email = '' or email like '%@local.invalid');

-- Realtime en Supabase: habilitar tabla incidents en Database -> Replication.
