import { BaseEl } from '/chat/static/js/base.js';
import { html, css } from '/chat/static/js/lit-core.min.js';

class JobQueueDashboard extends BaseEl {
  static properties = {
    jobs: { type: Array },
    stats: { type: Object },
    statusFilter: { type: String },
    jobTypeFilter: { type: String },
    loading: { type: Boolean },
    selectedJob: { type: Object },
    showJobDetail: { type: Boolean },
    errorMessage: { type: String }
  };

  constructor() {
    super();
    this.jobs = [];
    this.stats = { queued: 0, active: 0, completed: 0, failed: 0, total: 0 };
    this.statusFilter = '';
    this.jobTypeFilter = '';
    this.loading = true;
    this.selectedJob = null;
    this.showJobDetail = false;
    this.errorMessage = '';
  }

  static styles = css`
    :host {
      display: block;
      width: 100%;
    }
    
    .error-message {
      background-color: var(--danger-light);
      color: var(--danger);
      padding: 1rem;
      border-radius: 4px;
      margin-bottom: 1rem;
    }
    
    .job-list {
      width: 100%;
      border-collapse: collapse;
      margin-top: 1rem;
    }
    
    .job-list th,
    .job-list td {
      padding: 0.75rem;
      text-align: left;
      border-bottom: 1px solid var(--border-color);
    }
    
    .job-list th {
      background-color: var(--background-secondary);
      font-weight: 600;
    }
    
    .job-list tr:hover {
      background-color: var(--background-hover);
    }
    
    .job-actions {
      display: flex;
      gap: 0.5rem;
    }
    
    .empty-state {
      padding: 3rem;
      text-align: center;
      color: var(--text-secondary);
    }
    
    .table-container {
      overflow-x: auto;
    }
    
    .job-id {
      font-family: monospace;
      font-size: 0.9em;
    }
    
    .job-detail-field {
      margin-bottom: 1.5rem;
    }
    
    .job-detail-label {
      font-weight: 600;
      margin-bottom: 0.5rem;
      color: var(--text-secondary);
    }
    
    .job-detail-value {
      padding: 0.75rem;
      background-color: var(--background-secondary);
      border-radius: 4px;
      font-family: monospace;
      white-space: pre-wrap;
      word-break: break-word;
    }
    
    .loading-indicator {
      display: flex;
      justify-content: center;
      padding: 2rem;
    }
    
    .spinner {
      border: 4px solid rgba(0, 0, 0, 0.1);
      border-radius: 50%;
      border-top: 4px solid var(--primary);
      width: 40px;
      height: 40px;
      animation: spin 1s linear infinite;
    }
    
    @keyframes spin {
      0% { transform: rotate(0deg); }
      100% { transform: rotate(360deg); }
    }
    
    .refresh-btn {
      margin-bottom: 1rem;
    }
  `;

  connectedCallback() {
    super.connectedCallback();
    this.loadData();
    
    // Set up auto-refresh every 30 seconds
    this.refreshInterval = setInterval(() => {
      this.loadData();
    }, 30000);
  }
  
  disconnectedCallback() {
    super.disconnectedCallback();
    if (this.refreshInterval) {
      clearInterval(this.refreshInterval);
    }
  }

  async loadData() {
    this.loading = true;
    this.errorMessage = '';
    
    try {
      // Load stats
      const statsResponse = await fetch('/api/stats');
      if (statsResponse.ok) {
        this.stats = await statsResponse.json();
      } else {
        const error = await statsResponse.json();
        console.error('Error fetching stats:', error);
      }
      
      // Load jobs
      const queryParams = new URLSearchParams();
      if (this.statusFilter) queryParams.append('status', this.statusFilter);
      if (this.jobTypeFilter) queryParams.append('job_type', this.jobTypeFilter);
      
      const jobsResponse = await fetch(`/api/jobs?${queryParams}`);
      if (jobsResponse.ok) {
        this.jobs = await jobsResponse.json();
      } else {
        const error = await jobsResponse.json();
        console.error('Error fetching jobs:', error);
        this.errorMessage = `Error loading jobs: ${error.error || 'Unknown error'}`;
      }
    } catch (error) {
      console.error('Error loading data:', error);
      this.errorMessage = `Error: ${error.message || 'Unknown error'}`;
    } finally {
      this.loading = false;
    }
  }

  handleRefresh() {
    this.loadData();
  }

  handleStatusFilterChange(e) {
    this.statusFilter = e.target.value;
    this.loadData();
  }

  handleJobTypeFilterChange(e) {
    this.jobTypeFilter = e.target.value;
    this.loadData();
  }

  async handleViewJob(jobId) {
    try {
      const response = await fetch(`/api/jobs/${jobId}`);
      if (response.ok) {
        this.selectedJob = await response.json();
        this.showJobDetail = true;
      } else {
        const error = await response.json();
        console.error(`Error fetching job ${jobId}:`, error);
        this.errorMessage = `Error loading job: ${error.error || 'Unknown error'}`;
      }
    } catch (error) {
      console.error(`Error fetching job ${jobId}:`, error);
      this.errorMessage = `Error: ${error.message || 'Unknown error'}`;
    }
  }

  async handleCancelJob(jobId) {
    if (!confirm(`Are you sure you want to cancel job ${jobId}?`)) {
      return;
    }
    
    try {
      const response = await fetch(`/api/jobs/${jobId}`, {
        method: 'DELETE'
      });
      
      if (response.ok) {
        // Refresh data
        this.loadData();
      } else {
        const error = await response.json();
        console.error(`Error canceling job ${jobId}:`, error);
        this.errorMessage = `Error canceling job: ${error.error || 'Unknown error'}`;
      }
    } catch (error) {
      console.error(`Error canceling job ${jobId}:`, error);
      this.errorMessage = `Error: ${error.message || 'Unknown error'}`;
    }
  }

  closeJobDetail() {
    this.showJobDetail = false;
    this.selectedJob = null;
  }

  formatDate(dateString) {
    if (!dateString) return '-';
    const date = new Date(dateString);
    return date.toLocaleString();
  }

  formatStatus(status) {
    if (!status) return '';
    return status.charAt(0).toUpperCase() + status.slice(1);
  }

  renderJobDetail() {
    if (!this.selectedJob) return html``;
    
    return html`
      <div class="job-detail-modal ${this.showJobDetail ? '' : 'hidden'}">
        <div class="modal-content">
          <button class="modal-close" @click=${this.closeJobDetail}>&times;</button>
          
          <h2>Job Details</h2>
          
          <div class="job-detail-field">
            <div class="job-detail-label">Job ID</div>
            <div class="job-detail-value">${this.selectedJob.id}</div>
          </div>
          
          <div class="job-detail-field">
            <div class="job-detail-label">Status</div>
            <div class="job-detail-value">
              <span class="status-badge status-${this.selectedJob.status}">
                ${this.formatStatus(this.selectedJob.status)}
              </span>
            </div>
          </div>
          
          <div class="job-detail-field">
            <div class="job-detail-label">Agent</div>
            <div class="job-detail-value">${this.selectedJob.agent_name}</div>
          </div>
          
          <div class="job-detail-field">
            <div class="job-detail-label">Submitted By</div>
            <div class="job-detail-value">${this.selectedJob.username}</div>
          </div>
          
          <div class="job-detail-field">
            <div class="job-detail-label">Created At</div>
            <div class="job-detail-value">${this.formatDate(this.selectedJob.created_at)}</div>
          </div>
          
          ${this.selectedJob.started_at ? html`
            <div class="job-detail-field">
              <div class="job-detail-label">Started At</div>
              <div class="job-detail-value">${this.formatDate(this.selectedJob.started_at)}</div>
            </div>
          ` : ''}
          
          ${this.selectedJob.completed_at ? html`
            <div class="job-detail-field">
              <div class="job-detail-label">Completed At</div>
              <div class="job-detail-value">${this.formatDate(this.selectedJob.completed_at)}</div>
            </div>
          ` : ''}
          
          <div class="job-detail-field">
            <div class="job-detail-label">Instructions</div>
            <div class="job-detail-value">${this.selectedJob.instructions}</div>
          </div>
          
          ${this.selectedJob.job_type ? html`
            <div class="job-detail-field">
              <div class="job-detail-label">Job Type</div>
              <div class="job-detail-value">${this.selectedJob.job_type}</div>
            </div>
          ` : ''}
          
          ${this.selectedJob.log_id ? html`
            <div class="job-detail-field">
              <div class="job-detail-label">Log ID</div>
              <div class="job-detail-value">
                ${this.selectedJob.log_id}
                <a href="/chat/logs/${this.selectedJob.log_id}" target="_blank" class="btn btn-secondary" style="margin-left: 10px;">
                  View Log
                </a>
              </div>
            </div>
          ` : ''}
          
          ${this.selectedJob.result ? html`
            <div class="job-detail-field">
              <div class="job-detail-label">Result</div>
              <div class="job-detail-value">${this.selectedJob.result}</div>
            </div>
          ` : ''}
          
          ${this.selectedJob.error ? html`
            <div class="job-detail-field">
              <div class="job-detail-label">Error</div>
              <div class="job-detail-value">${this.selectedJob.error}</div>
            </div>
          ` : ''}
          
          ${this.selectedJob.metadata && Object.keys(this.selectedJob.metadata).length > 0 ? html`
            <div class="job-detail-field">
              <div class="job-detail-label">Metadata</div>
              <div class="job-detail-value">${JSON.stringify(this.selectedJob.metadata, null, 2)}</div>
            </div>
          ` : ''}
        </div>
      </div>
    `;
  }

  _render() {
    return html`
      ${this.errorMessage ? html`
        <div class="error-message">${this.errorMessage}</div>
      ` : ''}
      
      <div class="stats-cards">
        <div class="stat-card">
          <div class="stat-value">${this.stats.total}</div>
          <div class="stat-label">Total Jobs</div>
        </div>
        <div class="stat-card">
          <div class="stat-value">${this.stats.queued}</div>
          <div class="stat-label">Queued</div>
        </div>
        <div class="stat-card">
          <div class="stat-value">${this.stats.active}</div>
          <div class="stat-label">Active</div>
        </div>
        <div class="stat-card">
          <div class="stat-value">${this.stats.completed}</div>
          <div class="stat-label">Completed</div>
        </div>
        <div class="stat-card">
          <div class="stat-value">${this.stats.failed}</div>
          <div class="stat-label">Failed</div>
        </div>
      </div>
      
      <div class="section-header">
        <h2 class="section-title">Jobs</h2>
        <button class="btn btn-primary refresh-btn" @click=${this.handleRefresh}>
          Refresh
        </button>
      </div>
      
      <div class="job-filters">
        <select 
          class="filter-select" 
          @change=${this.handleStatusFilterChange}
          .value=${this.statusFilter}
        >
          <option value="">All Statuses</option>
          <option value="queued">Queued</option>
          <option value="active">Active</option>
          <option value="completed">Completed</option>
          <option value="failed">Failed</option>
        </select>
        
        <select 
          class="filter-select" 
          @change=${this.handleJobTypeFilterChange}
          .value=${this.jobTypeFilter}
        >
          <option value="">All Job Types</option>
          ${Array.from(new Set(this.jobs.map(job => job.job_type))).filter(Boolean).map(jobType => 
            html`<option value=${jobType}>${jobType}</option>`
          )}
        </select>
      </div>
      
      ${this.loading ? html`
        <div class="loading-indicator">
          <div class="spinner"></div>
        </div>
      ` : this.jobs.length === 0 ? html`
        <div class="empty-state">
          No jobs found matching your filters.
        </div>
      ` : html`
        <div class="table-container">
          <table class="job-list">
            <thead>
              <tr>
                <th>ID</th>
                <th>Status</th>
                <th>Agent</th>
                <th>Type</th>
                <th>Created</th>
                <th>User</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody>
              ${this.jobs.map(job => html`
                <tr>
                  <td class="job-id">${job.id}</td>
                  <td>
                    <span class="status-badge status-${job.status}">
                      ${this.formatStatus(job.status)}
                    </span>
                  </td>
                  <td>${job.agent_name}</td>
                  <td>${job.job_type || '-'}</td>
                  <td>${this.formatDate(job.created_at)}</td>
                  <td>${job.username}</td>
                  <td>
                    <div class="job-actions">
                      <button 
                        class="btn btn-secondary" 
                        @click=${() => this.handleViewJob(job.id)}
                      >
                        View
                      </button>
                      ${job.status === 'queued' ? html`
                        <button 
                          class="btn btn-danger" 
                          @click=${() => this.handleCancelJob(job.id)}
                        >
                          Cancel
                        </button>
                      ` : ''}
                    </div>
                  </td>
                </tr>
              `)}
            </tbody>
          </table>
        </div>
      `}
      
      ${this.renderJobDetail()}
    `;
  }
}

customElements.define('job-queue-dashboard', JobQueueDashboard);
