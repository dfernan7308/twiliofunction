import { createClient } from 'https://cdn.jsdelivr.net/npm/@supabase/supabase-js@2/+esm';

const state = {
  token: localStorage.getItem('appToken') || '',
  user: JSON.parse(localStorage.getItem('appUser') || 'null'),
  incidents: [],
  users: [],
  supabase: null,
  realtimeChannel: null
};

const elements = {
  loginCard: document.getElementById('loginCard'),
  appPanel: document.getElementById('appPanel'),
  loginForm: document.getElementById('loginForm'),
  loginError: document.getElementById('loginError'),
  sessionInfo: document.getElementById('sessionInfo'),
  logoutBtn: document.getElementById('logoutBtn'),
  incidentList: document.getElementById('incidentList'),
  adminPanel: document.getElementById('adminPanel'),
  userInfoPanel: document.getElementById('userInfoPanel'),
  createUserForm: document.getElementById('createUserForm'),
  adminError: document.getElementById('adminError'),
  userList: document.getElementById('userList'),
  userInfoList: document.getElementById('userInfoList')
};

const isAdmin = () => Boolean(state.user && state.user.role === 'admin');

const escapeHtml = (value) => String(value || '')
  .replace(/&/g, '&amp;')
  .replace(/</g, '&lt;')
  .replace(/>/g, '&gt;')
  .replace(/"/g, '&quot;')
  .replace(/'/g, '&#39;');

const formatDate = (value) => {
  if (!value) {
    return '-';
  }

  return new Date(value).toLocaleString('es-CL');
};

const apiFetch = async (url, options = {}) => {
  const headers = {
    'Content-Type': 'application/json',
    ...(options.headers || {})
  };

  if (state.token) {
    headers.Authorization = `Bearer ${state.token}`;
  }

  const response = await fetch(url, {
    ...options,
    headers
  });

  const data = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(data.error || 'Request failed');
  }

  return data;
};

const renderIncidents = () => {
  if (!state.incidents.length) {
    elements.incidentList.innerHTML = '<li class="incident-item">Aun no hay incidentes.</li>';
    return;
  }

  const adminCanDelete = isAdmin();

  elements.incidentList.innerHTML = state.incidents
    .map((incident) => {
      const linkedUser = incident.called_user_name || 'Sin coincidencia';
      const attendedLabel = incident.incident_attended ? 'Si' : 'No';
      const title = escapeHtml(incident.incident_title || 'Sin titulo');
      const severity = escapeHtml(incident.incident_severity || 'UNKNOWN');
      const description = escapeHtml(incident.incident_description || '');
      const status = escapeHtml(incident.incident_status || 'OPEN');
      const calledNumber = escapeHtml(incident.called_number || '-');
      const linked = escapeHtml(linkedUser);
      const attended = escapeHtml(attendedLabel);
      const adminAction = adminCanDelete
        ? `<button class="danger incident-delete-btn" type="button" data-id="${incident.id}">Eliminar</button>`
        : '';

      return `
        <li class="incident-item">
          <div class="incident-head">
            <strong>${title}</strong>
            <span class="severity-chip">${severity}</span>
          </div>
          <p>${description}</p>
          <div class="incident-meta">
            Estado: ${status}<br />
            Numero llamado: ${calledNumber}<br />
            Usuario enlazado: ${linked}<br />
            Se atendio la incidencia: ${attended}<br />
            Fecha: ${formatDate(incident.created_at)}
          </div>
          <div class="item-actions">${adminAction}</div>
        </li>
      `;
    })
    .join('');
};

const renderUsers = () => {
  if (!state.users.length) {
    elements.userList.innerHTML = '<li class="user-item">No hay usuarios registrados.</li>';
    return;
  }

  elements.userList.innerHTML = state.users
    .map(
      (user) => `
      <li class="user-item">
        <form class="stack compact user-edit-form" data-id="${user.id}">
          <label>Nombre <input type="text" name="username" value="${escapeHtml(user.username)}" required /></label>
          <label>Correo <input type="email" name="email" value="${escapeHtml(user.email || '')}" required /></label>
          <label>Numero <input type="text" name="phone" value="${escapeHtml(user.phone || '')}" required /></label>
          <label>
            Rol
            <select name="role">
              <option value="user" ${user.role === 'user' ? 'selected' : ''}>user</option>
              <option value="admin" ${user.role === 'admin' ? 'selected' : ''}>admin</option>
            </select>
          </label>
          <label>Actualizar contraseña (opcional)
            <input type="password" name="password" placeholder="Dejar vacio para no cambiar" />
          </label>
          <label class="inline-option">
            <input type="checkbox" name="is_active" ${user.is_active ? 'checked' : ''} />
            Activo
          </label>
          <div class="item-actions">
            <button type="submit">Guardar</button>
            <button type="button" class="danger user-delete-btn" data-id="${user.id}">Eliminar</button>
          </div>
        </form>
      </li>
    `
    )
    .join('');
};

const renderCurrentUserInfo = () => {
  if (!state.user) {
    elements.userInfoList.innerHTML = '<li class="user-item">Sin información de sesión.</li>';
    return;
  }

  const username = escapeHtml(state.user.username || '-');
  const email = escapeHtml(state.user.email || '-');
  const phone = escapeHtml(state.user.phone || '-');

  elements.userInfoList.innerHTML = `
    <li class="user-item">
      <strong>${username}</strong><br />
      Correo: ${email}<br />
      Numero: ${phone}
    </li>
  `;
};

const fetchIncidents = async () => {
  const result = await apiFetch('/api/incidents-list');
  state.incidents = result.incidents || [];
  renderIncidents();
};

const fetchUsers = async () => {
  if (!isAdmin()) {
    return;
  }

  const result = await apiFetch('/api/users-list');
  state.users = result.users || [];
  renderUsers();
};

const disconnectRealtime = () => {
  if (state.supabase && state.realtimeChannel) {
    state.supabase.removeChannel(state.realtimeChannel);
  }
  state.realtimeChannel = null;
};

const connectRealtime = async () => {
  disconnectRealtime();

  const config = await apiFetch('/api/public-config', { method: 'GET' });
  if (!config.supabaseUrl || !config.supabaseAnonKey) {
    return;
  }

  state.supabase = createClient(config.supabaseUrl, config.supabaseAnonKey);

  state.realtimeChannel = state.supabase
    .channel('incidents-feed')
    .on(
      'postgres_changes',
      {
        event: '*',
        schema: 'public',
        table: 'incidents'
      },
      (payload) => {
        if (payload.eventType === 'INSERT') {
          state.incidents = [payload.new, ...state.incidents].slice(0, 200);
        }

        if (payload.eventType === 'UPDATE') {
          state.incidents = state.incidents.map((incident) => (incident.id === payload.new.id ? payload.new : incident));
        }

        if (payload.eventType === 'DELETE') {
          state.incidents = state.incidents.filter((incident) => incident.id !== payload.old.id);
        }

        renderIncidents();
      }
    )
    .subscribe();
};

const enterApp = async () => {
  elements.loginCard.classList.add('hidden');
  elements.appPanel.classList.remove('hidden');

  const displayIdentity = state.user.email || state.user.username;
  elements.sessionInfo.textContent = `${displayIdentity} (${state.user.role})`;

  const adminView = isAdmin();
  elements.adminPanel.classList.toggle('hidden', !adminView);
  elements.userInfoPanel.classList.toggle('hidden', adminView);

  await fetchIncidents();
  if (adminView) {
    await fetchUsers();
  } else {
    renderCurrentUserInfo();
  }

  await connectRealtime();
};

const logout = () => {
  disconnectRealtime();
  state.token = '';
  state.user = null;
  state.incidents = [];
  state.users = [];
  localStorage.removeItem('appToken');
  localStorage.removeItem('appUser');

  elements.appPanel.classList.add('hidden');
  elements.loginCard.classList.remove('hidden');
  elements.loginForm.reset();
  elements.loginError.textContent = '';
  elements.userInfoList.innerHTML = '';
};

elements.loginForm.addEventListener('submit', async (event) => {
  event.preventDefault();
  elements.loginError.textContent = '';

  const formData = new FormData(elements.loginForm);
  const payload = {
    email: String(formData.get('email') || '').trim(),
    password: String(formData.get('password') || '').trim()
  };

  try {
    const result = await apiFetch('/api/auth-login', {
      method: 'POST',
      body: JSON.stringify(payload)
    });

    state.token = result.token;
    state.user = result.user;

    localStorage.setItem('appToken', state.token);
    localStorage.setItem('appUser', JSON.stringify(state.user));

    await enterApp();
  } catch (error) {
    elements.loginError.textContent = error.message;
  }
});

elements.createUserForm.addEventListener('submit', async (event) => {
  event.preventDefault();
  elements.adminError.textContent = '';

  const formData = new FormData(elements.createUserForm);
  const payload = {
    username: String(formData.get('username') || '').trim(),
    email: String(formData.get('email') || '').trim(),
    password: String(formData.get('password') || '').trim(),
    phone: String(formData.get('phone') || '').trim(),
    role: String(formData.get('role') || 'user').trim()
  };

  try {
    await apiFetch('/api/users-create', {
      method: 'POST',
      body: JSON.stringify(payload)
    });

    elements.createUserForm.reset();
    await fetchUsers();
  } catch (error) {
    elements.adminError.textContent = error.message;
  }
});

elements.userList.addEventListener('submit', async (event) => {
  const form = event.target.closest('.user-edit-form');
  if (!form) {
    return;
  }

  event.preventDefault();
  elements.adminError.textContent = '';

  const formData = new FormData(form);
  const payload = {
    id: form.dataset.id,
    username: String(formData.get('username') || '').trim(),
    email: String(formData.get('email') || '').trim(),
    phone: String(formData.get('phone') || '').trim(),
    role: String(formData.get('role') || 'user').trim(),
    password: String(formData.get('password') || '').trim(),
    is_active: formData.get('is_active') === 'on'
  };

  try {
    await apiFetch('/api/users-update', {
      method: 'POST',
      body: JSON.stringify(payload)
    });

    await fetchUsers();
  } catch (error) {
    elements.adminError.textContent = error.message;
  }
});

elements.userList.addEventListener('click', async (event) => {
  const deleteButton = event.target.closest('.user-delete-btn');
  if (!deleteButton) {
    return;
  }

  if (!window.confirm('Se eliminará el usuario seleccionado. ¿Continuar?')) {
    return;
  }

  elements.adminError.textContent = '';

  try {
    await apiFetch('/api/users-delete', {
      method: 'POST',
      body: JSON.stringify({ id: deleteButton.dataset.id })
    });

    await fetchUsers();
  } catch (error) {
    elements.adminError.textContent = error.message;
  }
});

elements.incidentList.addEventListener('click', async (event) => {
  const deleteButton = event.target.closest('.incident-delete-btn');
  if (!deleteButton || !isAdmin()) {
    return;
  }

  if (!window.confirm('Se eliminará la alerta del panel. ¿Continuar?')) {
    return;
  }

  try {
    await apiFetch('/api/incidents-delete', {
      method: 'POST',
      body: JSON.stringify({ id: Number(deleteButton.dataset.id) })
    });

    state.incidents = state.incidents.filter((incident) => incident.id !== Number(deleteButton.dataset.id));
    renderIncidents();
  } catch (error) {
    elements.adminError.textContent = error.message;
  }
});

elements.logoutBtn.addEventListener('click', logout);

if (state.token && state.user) {
  enterApp().catch((error) => {
    elements.loginError.textContent = error.message;
    logout();
  });
}
