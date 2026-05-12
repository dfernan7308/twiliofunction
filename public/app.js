import { createClient } from 'https://cdn.jsdelivr.net/npm/@supabase/supabase-js@2/+esm';

const state = {
  token: localStorage.getItem('appToken') || '',
  user: JSON.parse(localStorage.getItem('appUser') || 'null'),
  incidents: [],
  users: [],
  areas: [],
  adminViewTab: 'incidents',
  supabase: null,
  realtimeChannel: null,
  incidentView: {
    search: '',
    groupBy: 'none',
    status: '',
    severity: '',
    attended: 'all',
    from: '',
    to: '',
    perPage: 10,
    page: 1
  }
};

const elements = {
  loginCard: document.getElementById('loginCard'),
  appPanel: document.getElementById('appPanel'),
  loginForm: document.getElementById('loginForm'),
  loginError: document.getElementById('loginError'),
  sessionInfo: document.getElementById('sessionInfo'),
  logoutBtn: document.getElementById('logoutBtn'),
  adminTabs: document.getElementById('adminTabs'),
  tabIncidentsBtn: document.getElementById('tabIncidentsBtn'),
  tabUsersBtn: document.getElementById('tabUsersBtn'),
  appGrid: document.getElementById('appGrid'),
  incidentsPanelCard: document.getElementById('incidentsPanelCard'),
  incidentList: document.getElementById('incidentList'),
  incidentSearchInput: document.getElementById('incidentSearchInput'),
  incidentGroupBy: document.getElementById('incidentGroupBy'),
  incidentStatusFilter: document.getElementById('incidentStatusFilter'),
  incidentSeverityFilter: document.getElementById('incidentSeverityFilter'),
  incidentAttendedFilter: document.getElementById('incidentAttendedFilter'),
  incidentFromDate: document.getElementById('incidentFromDate'),
  incidentToDate: document.getElementById('incidentToDate'),
  incidentPageSize: document.getElementById('incidentPageSize'),
  incidentClearFilters: document.getElementById('incidentClearFilters'),
  incidentPrevPage: document.getElementById('incidentPrevPage'),
  incidentNextPage: document.getElementById('incidentNextPage'),
  incidentPageIndicator: document.getElementById('incidentPageIndicator'),
  incidentPaginationInfo: document.getElementById('incidentPaginationInfo'),
  adminPanel: document.getElementById('adminPanel'),
  userInfoPanel: document.getElementById('userInfoPanel'),
  createUserForm: document.getElementById('createUserForm'),
  createUserAreaSelect: document.getElementById('createUserAreaSelect'),
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

const toTimestamp = (value) => {
  if (!value) {
    return null;
  }

  const parsed = new Date(value).getTime();
  return Number.isFinite(parsed) ? parsed : null;
};

const normalizeFilterText = (value) => String(value || '').trim().toLowerCase();

const syncIncidentViewFromControls = () => {
  state.incidentView.search = normalizeFilterText(elements.incidentSearchInput.value);
  state.incidentView.groupBy = String(elements.incidentGroupBy.value || 'none');
  state.incidentView.status = normalizeFilterText(elements.incidentStatusFilter.value);
  state.incidentView.severity = normalizeFilterText(elements.incidentSeverityFilter.value);
  state.incidentView.attended = String(elements.incidentAttendedFilter.value || 'all');
  state.incidentView.from = String(elements.incidentFromDate.value || '');
  state.incidentView.to = String(elements.incidentToDate.value || '');

  const perPage = parseInt(String(elements.incidentPageSize.value || '10'), 10);
  state.incidentView.perPage = [5, 10, 15, 20].includes(perPage) ? perPage : 10;
};

const getFilteredIncidents = () => {
  const fromTs = toTimestamp(state.incidentView.from);
  const toTs = toTimestamp(state.incidentView.to);

  return state.incidents.filter((incident) => {
    const searchableValues = [
      incident.incident_title,
      incident.incident_description,
      incident.incident_status,
      incident.incident_severity,
      incident.called_user_name,
      incident.called_number,
      incident.problem_id,
      incident.cause_name,
      incident.affected_entity,
      incident.incident_area
    ]
      .map((value) => normalizeFilterText(value))
      .join(' ');

    if (state.incidentView.search && !searchableValues.includes(state.incidentView.search)) {
      return false;
    }

    const status = normalizeFilterText(incident.incident_status);
    if (state.incidentView.status && !status.includes(state.incidentView.status)) {
      return false;
    }

    const severity = normalizeFilterText(incident.incident_severity);
    if (state.incidentView.severity && !severity.includes(state.incidentView.severity)) {
      return false;
    }

    if (state.incidentView.attended === 'yes' && !incident.incident_attended) {
      return false;
    }

    if (state.incidentView.attended === 'no' && incident.incident_attended) {
      return false;
    }

    const incidentTs = toTimestamp(incident.created_at);
    if (fromTs !== null && (incidentTs === null || incidentTs < fromTs)) {
      return false;
    }

    if (toTs !== null && (incidentTs === null || incidentTs > toTs)) {
      return false;
    }

    return true;
  });
};

const buildIncidentGroups = (incidents) => {
  const groupBy = state.incidentView.groupBy;
  if (groupBy === 'none') {
    return [{ label: '', items: incidents }];
  }

  const groups = new Map();

  incidents.forEach((incident) => {
    let key = 'General';

    if (groupBy === 'severity') {
      key = incident.incident_severity || 'UNKNOWN';
    }

    if (groupBy === 'status') {
      key = incident.incident_status || 'OPEN';
    }

    if (groupBy === 'attended') {
      key = incident.incident_attended ? 'Atendida: Sí' : 'Atendida: No';
    }

    if (groupBy === 'user') {
      key = incident.called_user_name || 'Sin usuario enlazado';
    }

    if (groupBy === 'day') {
      key = incident.created_at ? new Date(incident.created_at).toLocaleDateString('es-CL') : 'Sin fecha';
    }

    if (groupBy === 'area') {
      key = incident.incident_area || 'Sin area';
    }

    if (!groups.has(key)) {
      groups.set(key, []);
    }

    groups.get(key).push(incident);
  });

  return Array.from(groups.entries()).map(([label, items]) => ({ label, items }));
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
  syncIncidentViewFromControls();
  const adminCanDelete = isAdmin();

  const filteredIncidents = getFilteredIncidents();
  const totalItems = filteredIncidents.length;
  const perPage = state.incidentView.perPage;
  const totalPages = Math.max(1, Math.ceil(totalItems / perPage));

  if (state.incidentView.page > totalPages) {
    state.incidentView.page = totalPages;
  }

  const page = state.incidentView.page;
  const startIndex = totalItems ? (page - 1) * perPage : 0;
  const pageIncidents = filteredIncidents.slice(startIndex, startIndex + perPage);
  const groupedIncidents = buildIncidentGroups(pageIncidents);

  if (!totalItems) {
    elements.incidentList.innerHTML = '<li class="incident-item">No hay incidentes para los filtros seleccionados.</li>';
  } else {
    elements.incidentList.innerHTML = groupedIncidents
      .map((group) => {
        const groupTitle = group.label
          ? `<li class="incident-group-header">Grupo: ${escapeHtml(group.label)} (${group.items.length})</li>`
          : '';

        const items = group.items
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
      const causeNameRaw = escapeHtml(incident.cause_name || '');
      const affectedEntityRaw = escapeHtml(incident.affected_entity || '');
      const incidentAreaRaw = escapeHtml(incident.incident_area || '');
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
            ${incidentAreaRaw ? `Area: <strong>${incidentAreaRaw}</strong><br />` : ''}
            ${causeNameRaw ? `Causa raíz: <strong>${causeNameRaw}</strong><br />` : ''}
            ${affectedEntityRaw ? `Impactado: <strong>${affectedEntityRaw}</strong><br />` : ''}
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

        return `${groupTitle}${items}`;
      })
      .join('');
  }

  const pageStart = totalItems ? startIndex + 1 : 0;
  const pageEnd = totalItems ? Math.min(startIndex + perPage, totalItems) : 0;
  elements.incidentPaginationInfo.textContent = `Mostrando ${pageStart}-${pageEnd} de ${totalItems}`;
  elements.incidentPageIndicator.textContent = `Página ${page}/${totalPages}`;
  elements.incidentPrevPage.disabled = page <= 1;
  elements.incidentNextPage.disabled = page >= totalPages;
};

const buildAreaOptionsHtml = (selectedAreaId = '') => {
  const options = (state.areas || [])
    .filter((area) => area.is_active || String(area.id) === String(selectedAreaId || ''))
    .map((area) => {
      const selected = String(area.id) === String(selectedAreaId || '') ? 'selected' : '';
      const inactiveSuffix = area.is_active ? '' : ' [INACTIVA]';
      return `<option value="${escapeHtml(area.id)}" ${selected}>${escapeHtml(area.name)} (${escapeHtml(area.code)})${inactiveSuffix}</option>`;
    });

  if (!options.length) {
    return '<option value="">Sin areas disponibles</option>';
  }

  return ['<option value="">Selecciona un area</option>', ...options].join('');
};

const renderCreateUserAreaOptions = () => {
  if (!elements.createUserAreaSelect) {
    return;
  }

  elements.createUserAreaSelect.innerHTML = buildAreaOptionsHtml('');
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
            Area
            <select name="area_id" required>
              ${buildAreaOptionsHtml(user.area_id || '')}
            </select>
          </label>
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
  const areaName = escapeHtml((state.user.area && state.user.area.name) || state.user.area_name || '-');

  elements.userInfoList.innerHTML = `
    <li class="user-item">
      <strong>${username}</strong><br />
      Correo: ${email}<br />
      Numero: ${phone}<br />
      Area: ${areaName}
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

const fetchAreas = async () => {
  if (!isAdmin()) {
    return;
  }

  const result = await apiFetch('/api/areas-list');
  state.areas = result.areas || [];
  renderCreateUserAreaOptions();
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

const setAdminViewTab = (tab) => {
  const normalizedTab = tab === 'users' ? 'users' : 'incidents';
  state.adminViewTab = normalizedTab;

  if (!isAdmin()) {
    if (elements.adminTabs) {
      elements.adminTabs.classList.add('hidden');
    }
    elements.incidentsPanelCard.classList.remove('hidden');
    elements.adminPanel.classList.add('hidden');
    return;
  }

  if (elements.adminTabs) {
    elements.adminTabs.classList.remove('hidden');
  }

  const showUsers = normalizedTab === 'users';
  elements.incidentsPanelCard.classList.toggle('hidden', showUsers);
  elements.adminPanel.classList.toggle('hidden', !showUsers);
  if (elements.appGrid) {
    elements.appGrid.classList.toggle('users-only-layout', showUsers);
  }

  if (elements.tabIncidentsBtn) {
    elements.tabIncidentsBtn.classList.toggle('active', !showUsers);
  }

  if (elements.tabUsersBtn) {
    elements.tabUsersBtn.classList.toggle('active', showUsers);
  }
};

const enterApp = async () => {
  elements.loginCard.classList.add('hidden');
  elements.appPanel.classList.remove('hidden');

  const displayIdentity = state.user.email || state.user.username;
  elements.sessionInfo.textContent = `${displayIdentity} (${state.user.role})`;

  const adminView = isAdmin();
  elements.userInfoPanel.classList.toggle('hidden', adminView);

  await fetchIncidents();
  if (adminView) {
    await fetchAreas();
    await fetchUsers();
    setAdminViewTab(state.adminViewTab || 'incidents');
  } else {
    setAdminViewTab('incidents');
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
  state.areas = [];
  state.adminViewTab = 'incidents';
  localStorage.removeItem('appToken');
  localStorage.removeItem('appUser');

  elements.appPanel.classList.add('hidden');
  elements.loginCard.classList.remove('hidden');
  elements.loginForm.reset();
  elements.loginError.textContent = '';
  elements.userInfoList.innerHTML = '';
  if (elements.adminTabs) {
    elements.adminTabs.classList.add('hidden');
  }
  state.incidentView.page = 1;
};

const resetIncidentFilters = () => {
  elements.incidentSearchInput.value = '';
  elements.incidentGroupBy.value = 'none';
  elements.incidentStatusFilter.value = '';
  elements.incidentSeverityFilter.value = '';
  elements.incidentAttendedFilter.value = 'all';
  elements.incidentFromDate.value = '';
  elements.incidentToDate.value = '';
  elements.incidentPageSize.value = '10';
  state.incidentView.page = 1;
  renderIncidents();
};

const onIncidentFilterChanged = () => {
  state.incidentView.page = 1;
  renderIncidents();
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
    area_id: String(formData.get('area_id') || '').trim(),
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
    area_id: String(formData.get('area_id') || '').trim(),
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

elements.incidentSearchInput.addEventListener('input', onIncidentFilterChanged);
elements.incidentGroupBy.addEventListener('change', onIncidentFilterChanged);
elements.incidentStatusFilter.addEventListener('input', onIncidentFilterChanged);
elements.incidentSeverityFilter.addEventListener('input', onIncidentFilterChanged);
elements.incidentAttendedFilter.addEventListener('change', onIncidentFilterChanged);
elements.incidentFromDate.addEventListener('change', onIncidentFilterChanged);
elements.incidentToDate.addEventListener('change', onIncidentFilterChanged);
elements.incidentPageSize.addEventListener('change', onIncidentFilterChanged);
elements.incidentClearFilters.addEventListener('click', resetIncidentFilters);

elements.incidentPrevPage.addEventListener('click', () => {
  if (state.incidentView.page <= 1) {
    return;
  }

  state.incidentView.page -= 1;
  renderIncidents();
});

elements.incidentNextPage.addEventListener('click', () => {
  state.incidentView.page += 1;
  renderIncidents();
});

if (elements.tabIncidentsBtn) {
  elements.tabIncidentsBtn.addEventListener('click', () => setAdminViewTab('incidents'));
}

if (elements.tabUsersBtn) {
  elements.tabUsersBtn.addEventListener('click', () => setAdminViewTab('users'));
}

elements.logoutBtn.addEventListener('click', logout);

if (state.token && state.user) {
  enterApp().catch((error) => {
    elements.loginError.textContent = error.message;
    logout();
  });
}
