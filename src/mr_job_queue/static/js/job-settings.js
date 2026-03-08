import { BaseEl } from '/chat/static/js/base.js';
import { html, css } from '/chat/static/js/lit-core.min.js';

class JobQueueSettings extends BaseEl {
  static properties = {
    config: { type: Object },
    loading: { type: Boolean },
    newJobType: { type: String },
    queuePaused: { type: Boolean },
    workers: { type: Object },
    monitorStats: { type: Object },
    monitorLastUpdated: { type: String }
  };

  static styles = css`
    :host {
      display: block;
      padding: 1rem;
    }
    .form-group {
      margin-bottom: 1rem;
    }
    label {
      display: block;
      margin-bottom: 0.5rem;
      color: var(--text-secondary);
    }
    input, select {
      width: 100%;
      padding: 0.5rem;
      background: var(--input-bg);
      border: 1px solid var(--border-color);
      color: var(--text-color);
      border-radius: 4px;
    }
    button {
      background: var(--primary);
      color: white;
      border: none;
      padding: 0.5rem 1rem;
      border-radius: 4px;
      cursor: pointer;
      margin-right: 0.5rem;
    }
    button.secondary {
      background: var(--secondary, #666);
    }
    button.danger {
      background: var(--error, #e53e3e);
    }
    .limits-section {
      margin-top: 1.5rem;
      padding: 1rem;
      border: 1px solid var(--border-color);
      border-radius: 4px;
    }
    .limits-section h3 {
      margin-top: 0;
      margin-bottom: 1rem;
      color: var(--text-color);
    }
    .job-type-limits {
      display: grid;
      grid-template-columns: 1fr auto auto auto;
      gap: 0.5rem;
      align-items: center;
      margin-bottom: 0.5rem;
    }
    .job-type-limits label {
      margin: 0;
    }
    .job-type-limits input {
      width: 80px;
    }
    .add-type-row {
      display: flex;
      gap: 0.5rem;
      margin-top: 1rem;
    }
    .add-type-row input {
      flex: 1;
    }
    .pause-section {
      margin-bottom: 1.5rem;
      padding: 1rem;
      border: 2px solid var(--warning, #ed8936);
      border-radius: 4px;
      background: var(--warning-bg, rgba(237, 137, 54, 0.1));
    }
    .pause-section.paused {
      border-color: var(--error, #e53e3e);
      background: var(--error-bg, rgba(229, 62, 62, 0.1));
    }
    .pause-btn {
      font-size: 1.1em;
      padding: 0.75rem 1.5rem;
    }
    .pause-btn.paused {
      background: var(--success, #48bb78);
    }
    .section-divider {
      border-top: 1px solid var(--border-color);
      margin: 1.5rem 0;
    }
    .monitor-section {
      margin-top: 1.5rem;
      padding: 1rem;
      border: 1px solid var(--border-color);
      border-radius: 4px;
    }
    .monitor-section h3 {
      margin-top: 0;
      margin-bottom: 0.25rem;
      color: var(--text-color);
    }
    .monitor-updated {
      font-size: 0.75em;
      color: var(--text-secondary);
      margin-bottom: 1rem;
    }
    .monitor-table {
      width: 100%;
      border-collapse: collapse;
      font-size: 0.9em;
      margin-bottom: 1rem;
    }
    .monitor-table th {
      text-align: left;
      padding: 0.4rem 0.6rem;
      background: var(--background-secondary);
      color: var(--text-secondary);
      font-weight: 600;
      border-bottom: 1px solid var(--border-color);
    }
    .monitor-table td {
      padding: 0.4rem 0.6rem;
      border-bottom: 1px solid var(--border-color);
    }
    .monitor-table tr:last-child td {
      border-bottom: none;
    }
    .util-bar-bg {
      background: var(--border-color);
      border-radius: 3px;
      height: 8px;
      width: 80px;
      display: inline-block;
      vertical-align: middle;
      margin-right: 4px;
    }
    .util-bar-fill {
      height: 8px;
      border-radius: 3px;
      background: var(--primary);
      transition: width 0.3s;
    }
    .util-bar-fill.warn { background: var(--warning, #ed8936); }
    .util-bar-fill.full { background: var(--error, #e53e3e); }
    .worker-dead { color: var(--error, #e53e3e); }
    .worker-ok { color: var(--success, #48bb78); }
    .worker-idle { color: var(--text-secondary); }
  `;

  constructor() {
    super();
    this.config = { 
      mode: 'standalone', 
      master_url: '', 
      api_key: '',
      stale_job_timeout_minutes: 60,
      limits: {
        default: { max_global: 5, max_per_instance: 1 }
      }
    };
    this.loading = true;
    this.newJobType = '';
    this.queuePaused = false;
    this.workers = {};
    this.monitorStats = {};
    this.monitorLastUpdated = '';
    this.loadSettings();
    this.loadQueueStatus();
    this.loadMonitor();
    this._monitorInterval = setInterval(() => this.loadMonitor(), 10000);
  }

  async loadSettings() {
    try {
      const response = await fetch('/api/config');
      if (response.ok) {
        this.config = await response.json();
        // Ensure limits object exists
        if (!this.config.limits) {
          this.config.limits = { default: { max_global: 5, max_per_instance: 1 } };
        }
      }
    } finally {
      this.loading = false;
    }
  }

  async loadQueueStatus() {
    try {
      const response = await fetch('/api/queue/status');
      if (response.ok) {
        const data = await response.json();
        this.queuePaused = data.paused;
      }
    } catch (e) {
      console.error('Error loading queue status:', e);
    }
  }

  async loadMonitor() {
    try {
      const [statsRes, workersRes] = await Promise.all([
        fetch('/api/stats'),
        fetch('/api/workers')
      ]);
      if (statsRes.ok) this.monitorStats = await statsRes.json();
      if (workersRes.ok) this.workers = await workersRes.json();
      this.monitorLastUpdated = new Date().toLocaleTimeString();
    } catch (e) {
      console.error('Error loading monitor data:', e);
    }
  }

  async saveSettings() {
    try {
      const response = await fetch('/api/config', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(this.config)
      });
      if (response.ok) {
        alert('Settings saved successfully. Restart may be required for some changes.');
      }
    } catch (e) {
      alert('Error saving settings: ' + e);
    }
  }

  async togglePause() {
    try {
      const response = await fetch('/api/queue/pause', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ paused: !this.queuePaused })
      });
      if (response.ok) {
        const data = await response.json();
        this.queuePaused = data.paused;
      }
    } catch (e) {
      alert('Error toggling pause: ' + e);
    }
  }

  updateLimit(jobType, field, value) {
    if (!this.config.limits[jobType]) {
      this.config.limits[jobType] = { max_global: 5, max_per_instance: 1 };
    }
    this.config.limits[jobType][field] = parseInt(value) || 1;
    this.requestUpdate();
  }

  addJobType() {
    if (!this.newJobType.trim()) return;
    const typeName = this.newJobType.trim().toLowerCase().replace(/[^a-z0-9_.-]/g, '_');
    if (!this.config.limits[typeName]) {
      this.config.limits[typeName] = { max_global: 2, max_per_instance: 1 };
    }
    this.newJobType = '';
    this.requestUpdate();
  }

  removeJobType(jobType) {
    if (jobType === 'default') {
      alert('Cannot remove default limits');
      return;
    }
    delete this.config.limits[jobType];
    this.requestUpdate();
  }

  _render() {
    const jobTypes = Object.keys(this.config.limits || {});
    
    return html`
      <h3>Job Queue Configuration</h3>
      
      <div class="pause-section ${this.queuePaused ? 'paused' : ''}">
        <p style="margin: 0 0 0.5rem 0;">
          <strong>Queue Status:</strong> ${this.queuePaused ? 'PAUSED - No new jobs will be processed' : 'Running'}
        </p>
        <button class="pause-btn ${this.queuePaused ? 'paused' : ''}" @click=${this.togglePause}>
          ${this.queuePaused ? 'Resume Queue' : 'Pause Queue'}
        </button>
      </div>
      
      <div class="section-divider"></div>
      
      <div class="form-group">
        <label>Mode</label>
        <select .value=${this.config.mode} @change=${(e) => { this.config.mode = e.target.value; this.requestUpdate(); }}>
          <option value="standalone" ?selected=${this.config.mode === 'standalone'}>Standalone</option>
          <option value="master" ?selected=${this.config.mode === 'master'}>Master (accepts remote workers)</option>
          <option value="worker" ?selected=${this.config.mode === 'worker'}>Worker (connects to master)</option>
        </select>
      </div>
      
      ${this.config.mode === 'worker' ? html`
        <div class="form-group">
          <label>Master URL</label>
          <input type="text" .value=${this.config.master_url || ''} 
                 @input=${(e) => this.config.master_url = e.target.value.trim()}
                 placeholder="https://master-server.example.com">
        </div>
      ` : ''}
      
      ${this.config.mode !== 'standalone' ? html`
        <div class="form-group">
          <label>API Key (shared secret for auth)</label>
          <input type="password" .value=${this.config.api_key || ''} 
                 @input=${(e) => this.config.api_key = e.target.value.trim()}>
        </div>
      ` : ''}
      
      <div class="form-group">
        <label>Stale Job Timeout (minutes)</label>
        <input type="number" .value=${this.config.stale_job_timeout_minutes || 60} 
               @input=${(e) => this.config.stale_job_timeout_minutes = parseInt(e.target.value)}
               min="0">
        <small style="color: var(--text-secondary)">Jobs active longer than this are moved back to queue. Set to 0 to disable.</small>
      </div>
      
      <div class="section-divider"></div>
      
      <div class="limits-section">
        <h3>Concurrency Limits by Job Type</h3>
        <p style="color: var(--text-secondary); margin-bottom: 1rem;">
          <strong>Max Global:</strong> Total concurrent jobs across all instances<br>
          <strong>Max Per Instance:</strong> Concurrent jobs on this server
        </p>
        
        <div class="job-type-limits" style="font-weight: bold; margin-bottom: 0.5rem;">
          <span>Job Type</span>
          <span>Max Global</span>
          <span>Max/Instance</span>
          <span></span>
        </div>
        
        ${jobTypes.map(jobType => html`
          <div class="job-type-limits">
            <label>${jobType}</label>
            <input type="number" min="1" 
                   .value=${this.config.limits[jobType]?.max_global || 5}
                   @input=${(e) => this.updateLimit(jobType, 'max_global', e.target.value)}>
            <input type="number" min="1" 
                   .value=${this.config.limits[jobType]?.max_per_instance || 1}
                   @input=${(e) => this.updateLimit(jobType, 'max_per_instance', e.target.value)}>
            ${jobType !== 'default' ? html`
              <button class="danger" @click=${() => this.removeJobType(jobType)}>Remove</button>
            ` : html`<span></span>`}
          </div>
        `)}
        
        <div class="add-type-row">
          <input type="text" placeholder="New job type name" 
                 .value=${this.newJobType}
                 @input=${(e) => this.newJobType = e.target.value}
                 @keypress=${(e) => e.key === 'Enter' && this.addJobType()}>
          <button class="secondary" @click=${this.addJobType}>Add Type</button>
        </div>
      </div>
      
      <div class="section-divider"></div>
      
      <button @click=${this.saveSettings}>Save Configuration</button>

      ${this._renderMonitor()}
    `;
  }

  _renderMonitor() {
    const now = Date.now();
    const workerEntries = Object.entries(this.workers || {});
    const stats = this.monitorStats || {};
    const limits = (this.config && this.config.limits) || {};

    // Build per-type rows: collect all type keys from stats
    const typeKeys = Object.keys(stats)
      .filter(k => !['queued','active','completed','failed','total'].includes(k))
      .reduce((acc, k) => {
        // stats keys look like "active_call.KatieFullVerify" or "queued_verification"
        const m = k.match(/^(queued|active)_(.+)$/);
        if (m) acc.add(m[2]);
        return acc;
      }, new Set());

    // Also add types from limits config
    Object.keys(limits).forEach(t => typeKeys.add(t));

    const typeRows = Array.from(typeKeys).sort().map(t => {
      const queued = stats['queued_' + t] || 0;
      const active = stats['active_' + t] || 0;
      // find matching limit via prefix
      let lim = limits[t];
      if (!lim) {
        for (const prefix of Object.keys(limits)) {
          if (prefix !== 'default' && t.startsWith(prefix + '.')) { lim = limits[prefix]; break; }
        }
      }
      if (!lim) lim = limits['default'] || { max_global: 5, max_per_instance: 1 };
      const maxG = lim.max_global || 5;
      const maxI = lim.max_per_instance || 1;
      const pct = Math.min(100, Math.round(active / maxG * 100));
      const fillClass = pct >= 100 ? 'full' : pct >= 75 ? 'warn' : '';
      return html`
        <tr>
          <td>${t}</td>
          <td>${queued}</td>
          <td>${active}</td>
          <td>${maxG}</td>
          <td>${maxI}</td>
          <td>
            <span class="util-bar-bg">
              <div class="util-bar-fill ${fillClass}" style="width:${pct}%"></div>
            </span>
            ${active}/${maxG}
          </td>
        </tr>`;
    });

    const workerRows = workerEntries.map(([wid, info]) => {
      const lastSeen = info.last_seen ? new Date(info.last_seen) : null;
      const ageSec = lastSeen ? Math.round((now - lastSeen.getTime()) / 1000) : null;
      const ageStr = ageSec === null ? '?' : ageSec < 60 ? ageSec + 's ago' : Math.round(ageSec/60) + 'm ago';
      const statusClass = ageSec === null ? '' : ageSec > 600 ? 'worker-dead' : ageSec > 120 ? 'worker-idle' : 'worker-ok';
      const activeJobs = (info.active_jobs || []).length;
      return html`
        <tr>
          <td class="${statusClass}">${wid}</td>
          <td>${info.ip || '-'}</td>
          <td class="${statusClass}">${ageStr}</td>
          <td>${activeJobs}</td>
        </tr>`;
    });

    return html`
      <div class="monitor-section">
        <h3>Live Monitor</h3>
        <div class="monitor-updated">Updated: ${this.monitorLastUpdated || '...'} (auto-refresh 10s)</div>

        <strong>Workers</strong>
        <table class="monitor-table">
          <thead><tr><th>Worker ID</th><th>IP</th><th>Last Seen</th><th>Active Jobs</th></tr></thead>
          <tbody>${workerEntries.length ? workerRows : html`<tr><td colspan="4" style="color:var(--text-secondary)">No workers registered</td></tr>`}</tbody>
        </table>

        <strong>Per-Type Utilization</strong>
        <table class="monitor-table">
          <thead><tr><th>Type</th><th>Queued</th><th>Active</th><th>Max Global</th><th>Max/Inst</th><th>Utilization</th></tr></thead>
          <tbody>${typeRows.length ? typeRows : html`<tr><td colspan="6" style="color:var(--text-secondary)">No data</td></tr>`}</tbody>
        </table>
      </div>`;
  }
}
customElements.define('job-queue-settings', JobQueueSettings);
