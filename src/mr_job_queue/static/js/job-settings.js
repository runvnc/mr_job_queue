import { BaseEl } from '/chat/static/js/base.js';
import { html, css } from '/chat/static/js/lit-core.min.js';

class JobQueueSettings extends BaseEl {
  static properties = {
    config: { type: Object },
    loading: { type: Boolean }
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
    }
  `;

  constructor() {
    super();
    this.config = { max_concurrent: 5, master_url: '', mode: 'standalone', api_key: '' };
    this.loading = true;
    this.loadSettings();
  }

  async loadSettings() {
    try {
      const response = await fetch('/api/config');
      if (response.ok) {
        this.config = await response.json();
      }
    } finally {
      this.loading = false;
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
        alert('Settings saved successfully');
      }
    } catch (e) {
      alert('Error saving settings: ' + e);
    }
  }

  _render() {
    return html`
      <div class="form-group">
        <label>Mode</label>
        <select .value=${this.config.mode} @change=${(e) => this.config.mode = e.target.value}>
          <option value="standalone">Standalone</option>
          <option value="master">Master</option>
          <option value="worker">Worker</option>
        </select>
      </div>
      <div class="form-group">
        <label>Master URL (for Workers)</label>
        <input type="text" .value=${this.config.master_url} @input=${(e) => this.config.master_url = e.target.value}>
      </div>
      <div class="form-group">
        <label>API Key (for Master/Worker Auth)</label>
        <input type="password" .value=${this.config.api_key} @input=${(e) => this.config.api_key = e.target.value}>
      </div>
      <div class="form-group">
        <label>Max Concurrent Jobs</label>
        <input type="number" .value=${this.config.max_concurrent} @input=${(e) => this.config.max_concurrent = parseInt(e.target.value)}>
      </div>
      <button @click=${this.saveSettings}>Save Configuration</button>
    `;
  }
}
customElements.define('job-queue-settings', JobQueueSettings);
