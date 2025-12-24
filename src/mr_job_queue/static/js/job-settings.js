import { BaseEl } from '/chat/static/js/base.js';
import { html, css } from '/chat/static/js/lit-core.min.js';

class JobQueueSettings extends BaseEl {
  static properties = {
    config: { type: Object },
    loading: { type: Boolean },
    newJobType: { type: String },
    queuePaused: { type: Boolean }
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
    this.loadSettings();
    this.loadQueueStatus();
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
                 @input=${(e) => this.config.master_url = e.target.value}
                 placeholder="https://master-server.example.com">
        </div>
      ` : ''}
      
      ${this.config.mode !== 'standalone' ? html`
        <div class="form-group">
          <label>API Key (shared secret for auth)</label>
          <input type="password" .value=${this.config.api_key || ''} 
                 @input=${(e) => this.config.api_key = e.target.value}>
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
    `;
  }
}
customElements.define('job-queue-settings', JobQueueSettings);
