import { BaseEl } from '/chat/static/js/base.js';
import { html, css } from '/chat/static/js/lit-core.min.js';

class JobsList extends BaseEl {
  static properties = {
    jobs: { type: Array },
    loading: { type: Boolean },
    error: { type: String },
    statusFilter: { type: String },
    limit: { type: Number }
  };

  static styles = css`
    :host {
      display: block;
      margin-bottom: 2rem;
      width: 100%;
    }

    .jobs-container {
      background: var(--background-secondary, #2d3748);
      padding: 1.5rem;
      border-radius: 8px;
      box-shadow: 0 2px 4px rgba(0, 0, 0, 0.3);
      width: 100%;
      box-sizing: border-box;
      overflow-x: auto;
    }

    .jobs-header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 1.5rem;
      padding: 0 0.5rem;
    }

    .jobs-title {
      font-size: 1.25rem;
      font-weight: 600;
      color: var(--text-color, #f7fafc);
    }

    .jobs-actions {
      display: flex;
      gap: 0.75rem;
      align-items: center;
    }

    .filter-select {
      padding: 0.5rem 0.75rem;
      border-radius: 4px;
      border: 1px solid var(--border-color, #4a5568);
      background-color: var(--input-bg, #1a202c);
      color: var(--text-color, #f7fafc);
      font-size: 0.95rem;
      min-width: 150px;
    }

    .refresh-btn {
      background-color: var(--secondary, #a0aec0);
      color: white;
      border: none;
      padding: 0.5rem 1rem;
      border-radius: 4px;
      cursor: pointer;
      transition: background-color 0.2s;
      font-size: 0.95rem;
    }

    .refresh-btn:hover {
      background-color: var(--secondary-dark, #718096);
    }

    .jobs-table {
      width: 100%;
      border-collapse: collapse;
      margin-top: 1rem;
      table-layout: fixed;
      margin: 0;
    }

    .jobs-table th,
    .jobs-table td {
      padding: 0.25rem 0.25rem;
      font-size: 0.85rem;
      text-align: left;
      word-wrap: break-word;
      border-bottom: 1px solid var(--border-color, #4a5568);
    }

    .jobs-table th {
      font-weight: 600;
      color: var(--text-secondary, #cbd5e0);
      background-color: #1e2a3a;
    }

    .jobs-table tr:nth-child(odd) {
      background-color: rgba(0, 0, 0, 0.2);
    }

    .jobs-table tr:hover {
      background-color: var(--hover-bg, rgba(255, 255, 255, 0.05));
    }

    .status-icon {
      display: inline-block;
      width: 12px;
      height: 12px;
      margin-right: 4px;
      border-radius: 50%;
      margin-right: 4px;
    }

    .status-icon-queued {
      background-color: var(--info, #63b3ed);
    }

    .status-icon-active {
      background-color: var(--warning, #ed8936);
    }

    .status-icon-completed {
      background-color: var(--success, #48bb78);
    }

    .status-icon-failed {
      background-color: var(--error, #f56565);
    }

    .status-icon-wrapper {
      display: flex;
      align-items: center;
    }

    .status-icon-text {
      margin-left: 4px;
      font-size: 0.75rem;
    }

    /* Material icons for status */
    .material-icon {
      font-size: 16px;
      margin-right: 4px;
    }

    .icon-queued {
      color: var(--info, #63b3ed);
    }

    .icon-active {
      color: var(--warning, #ed8936);
    }

    .icon-completed {
      color: var(--success, #48bb78);
    }

    .icon-failed {
      color: var(--error, #f56565);
    }

    .clickable {
      cursor: pointer;
      text-decoration: underline;
      color: var(--primary, #667eea);
    }

    .status-badge {
      display: inline-block;
      padding: 0.25rem 0.5rem;
      border-radius: 4px;
      font-size: 0.75rem;
      font-weight: 600;
      text-transform: uppercase;
    }

    .status-queued {
      background-color: var(--info-light, #2a4365);
      color: var(--info, #63b3ed);
    }

    .status-active {
      background-color: var(--warning-light, #7b341e);
      color: var(--warning, #ed8936);
    }

    .status-completed {
      background-color: var(--success-light, #1c4532);
      color: var(--success, #48bb78);
    }

    .status-failed {
      background-color: var(--error-light, #742a2a);
      color: var(--error, #f56565);
    }

    .job-link {
      color: var(--primary, #667eea);
      text-decoration: none;
      font-weight: 500;
    }

    .job-link:hover {
      text-decoration: underline;
    }

    .loading-indicator {
      display: flex;
      justify-content: center;
      padding: 2rem;
    }

    .spinner {
      border: 4px solid rgba(255, 255, 255, 0.1);
      border-radius: 50%;
      border-top: 4px solid var(--primary, #667eea);
      width: 24px;
      height: 24px;
      animation: spin 1s linear infinite;
    }

    @keyframes spin {
      0% { transform: rotate(0deg); }
      100% { transform: rotate(360deg); }
    }

    .empty-state {
      padding: 2rem;
      text-align: center;
      color: var(--text-secondary, #cbd5e0);
      font-size: 1rem;
    }

    .error-message {
      color: var(--error, #f56565);
      padding: 0.75rem;
      background-color: var(--error-bg, #742a2a);
      border-radius: 4px;
      border-left: 3px solid var(--error, #f56565);
      margin-bottom: 1rem;
    }

    .truncate {
      max-width: 200px;
      /* white-space: nowrap; */
      overflow: hidden;
      text-overflow: ellipsis;
      display: inline-block;
    }

    /* Table column widths */
    .col-id { width: 12%; }
    .col-status { width: 5%; }
    .col-job-type { width: 15%; max-width: 150px; }
    .col-agent { width: 10%; }
    .col-created { width: 12%; }
    .col-instructions { width: 28%; }
    .col-result { width: 10%; }

    /* Modal styles */
    .modal {
      display: none;
      position: fixed;
      z-index: 1000;
      left: 0;
      top: 0;
      width: 100%;
      height: 100%;
      overflow: auto;
      background-color: rgba(0, 0, 0, 0.7);
    }

    .modal.show {
      display: flex;
      align-items: center;
      justify-content: center;
    }

    .modal-content {
      background-color: var(--background-secondary, #2d3748);
      margin: auto;
      padding: 20px;
      border-radius: 8px;
      width: 80%;
      max-width: 800px;
      max-height: 80vh;
      overflow-y: auto;
      position: relative;
    }

    .close-modal {
      position: absolute;
      top: 10px;
      right: 15px;
      color: var(--text-color, #f7fafc);
      font-size: 28px;
      font-weight: bold;
      cursor: pointer;
    }

    .result-content {
      background-color: var(--background-primary, #1a202c);
      padding: 15px;
      max-height: 70vh;
      border-radius: 4px;
      margin-top: 10px;
      white-space: pre-wrap;
      font-family: monospace;
      font-size: 0.9rem;
    }

    .markdown-result {
      white-space: normal;
      font-family: inherit;
      line-height: 1.5;
    }
    
    .markdown-result h1, .markdown-result h2, .markdown-result h3 { margin-top: 1rem; margin-bottom: 0.5rem; }
    .markdown-result p { margin-bottom: 1rem; }
    .markdown-result pre { background: rgba(0,0,0,0.2); padding: 0.5rem; border-radius: 4px; overflow-x: auto; }
    .markdown-result code { font-family: monospace; background: rgba(0,0,0,0.2); padding: 0.1rem 0.3rem; border-radius: 3px; }
    .markdown-result ul, .markdown-result ol { margin-left: 1.5rem; margin-bottom: 1rem; }
    .markdown-result img { max-width: 100%; height: auto; }
    .markdown-result blockquote { border-left: 3px solid var(--border-color, #4a5568); padding-left: 1rem; margin-left: 0; color: var(--text-secondary, #cbd5e0); }

  `;

  constructor() {
    super();
    this.jobs = [];
    this.loading = true;
    this.error = '';
    this.statusFilter = '';
    this.limit = 100;
    this.loadJobs();
    console.log('JobsList initialized');

    // Listen for job submission events
    window.addEventListener('job-submitted', () => this.loadJobs());

    // Initialize modal element
    this.resultModal = null;
  }

  disconnectedCallback() {
    super.disconnectedCallback();
    window.removeEventListener('job-submitted', () => this.loadJobs());
    if (this.refreshInterval) {
      clearInterval(this.refreshInterval);
    }
  }

  connectedCallback() {
    super.connectedCallback();
    // Auto-refresh every 30 seconds
    this.refreshInterval = setInterval(() => {
      this.loadJobs();
    }, 30000);
  }

  async loadJobs() {
    this.loading = true;
    this.error = '';

    try {
      const queryParams = new URLSearchParams();
      if (this.statusFilter) {
        queryParams.append('status', this.statusFilter);
      }
      queryParams.append('limit', this.limit.toString());

      const response = await fetch(`/api/jobs?${queryParams}`);
      if (response.ok) {
        this.jobs = await response.json();
        console.log('Loaded jobs:', this.jobs);
      } else {
        const errorData = await response.json();
        this.error = errorData.error || 'Failed to load jobs';
        console.error('Error response from API:', errorData);
      }
    } catch (error) {
      console.error('Error loading jobs:', error);
      this.error = 'An error occurred while loading jobs';
    } finally {
      this.loading = false;
    }
  }

  handleRefresh() {
    this.loadJobs();
  }

  handleStatusFilterChange(e) {
    this.statusFilter = e.target.value;
    this.loadJobs();
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

  async showJobResult(jobId) {
    try {
      // First check if we already have a modal element
      const modalId = "job-output";
      if (!this.resultModal) {
        // Create modal elements if they don't exist
        this.resultModal = document.getElementById("job-output")
        // Add event listener to close button
        const closeBtn = this.resultModal.querySelector('.close-modal');
        closeBtn.addEventListener('click', () => {
          this.resultModal.close()
        });

        // Close modal when clicking outside of it
        this.resultModal.addEventListener('click', (e) => {
          if (e.target === this.resultModal) {
            this.resultModal.close()
          }
        });
      }

      // If modal doesn't exist in the DOM, create it
      if (!this.resultModal) {
        // Create the modal element
        this.resultModal = document.createElement('dialog');
        this.resultModal.id = modalId;
        this.resultModal.className = 'modal-content';
        
        // Add modal content
        this.resultModal.innerHTML = `
          <div class="modal-header">
            <h3>Job Result</h3>
            <button class="close-modal">&times;</button>
          </div>
          <div class="result-content"></div>
        `;
        
        // Add to document
        document.body.appendChild(this.resultModal);
        
        // Add event listeners
        this.resultModal.querySelector('.close-modal').addEventListener('click', () => this.resultModal.close());
      }

      // Fetch job result from completed directory
      const response = await fetch(`/api/jobs/${jobId}`);
      if (response.ok) {
        const jobData = await response.json();
        
        // Add marked.js script if not already loaded
        this.ensureMarkedJsLoaded();
        const result = jobData.result || 'No result available';
        const resultContent = this.resultModal.querySelector('.result-content');
        
        // Check if the result looks like markdown
        const isMarkdown = this.looksLikeMarkdown(result);
        
        if (isMarkdown) {
          try {
            // Try to use marked.js if available
            if (window.marked) {
              // Create a container with markdown class
              resultContent.innerHTML = '';
              const markdownDiv = document.createElement('div');
              markdownDiv.className = 'markdown-result';
              
              // Parse markdown and set as HTML
              markdownDiv.innerHTML = marked.parse(result);
              resultContent.appendChild(markdownDiv);
            } else {
              // Fallback if marked.js is not available
              resultContent.innerHTML = result;
              console.warn('Marked.js not available for markdown rendering');
            }
          } catch (error) {
            console.error('Error parsing markdown:', error);
            resultContent.innerHTML = result;
          }
        } else {
          // Not markdown, display as plain text
          resultContent.innerHTML = result;
        }
        
        this.resultModal.showModal();
      } else {
        console.error(`Error fetching job result for ${jobId}:`, await response.json());
      }
    } catch (error) {
      console.error('Error showing job result:', error);
    }
  }
  
  ensureMarkedJsLoaded() {
    if (window.marked) return; // Already loaded
    
    // Check if script is already being loaded
    if (document.querySelector('script[src*="marked"]')) return;
    
    // Create and append the script
    const script = document.createElement('script');
    script.src = 'https://cdn.jsdelivr.net/npm/marked/marked.min.js';
    script.async = true;
    script.onload = () => {
      console.log('Marked.js loaded successfully');
      // Configure marked options if needed
      if (window.marked) {
        marked.setOptions({ breaks: true }); // Enable GitHub Flavored Markdown
      }
    };
    document.head.appendChild(script);
  }
  
  looksLikeMarkdown(text) {
    if (!text || typeof text !== 'string') return false;
    
    // Check for common markdown patterns
    const markdownPatterns = [
      /^#+ .*$/m,                // Headers
      /\*\*.*\*\*/,              // Bold
      /\*.*\*/,                  // Italic
      /\[.*\]\(.*\)/,            // Links
      /^- .*$/m,                 // Unordered lists
      /^\d+\. .*$/m,             // Ordered lists
      /^```[\s\S]*?```$/m,       // Code blocks
      /^>.*$/m                   // Blockquotes
    ];
    
    return markdownPatterns.some(pattern => pattern.test(text));
  }

  _render() {
    return html`
      <div class="jobs-container">
        <div class="jobs-header">
          <h3 class="jobs-title">Recent Jobs</h3>
          <div class="jobs-actions">
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
            <button class="refresh-btn" @click=${this.handleRefresh}>
              Refresh
            </button>
          </div>
        </div>

        ${this.error ? html`
          <div class="error-message">${this.error}</div>
        ` : ''}

        ${this.loading ? html`
          <div class="loading-indicator">
            <div class="spinner"></div>
          </div>
        ` : this.jobs.length === 0 ? html`
          <div class="empty-state">
            No jobs found matching your filters.
          </div>
        ` : html`
          <div class="table-responsive">
            <table class="jobs-table">
              <thead>
                <tr>
                  <th class="col-id">ID</th>
                  <th class="col-status">Status</th>
                  <th class="col-job-type">Type</th>
                  <th class="col-agent">Agent</th>
                  <th class="col-created">Created</th>
                  <th class="col-instructions">Instructions</th>
                  <th class="col-result">Result/Session</th>
                </tr>
              </thead>
              <tbody>
                ${this.jobs.map(job => html`
                  <tr>
                    <td>${job.id}</td>
                    <td>
                      <div class="status-icon-wrapper">
                        <span class="material-icon icon-${job.status}">
                          ${job.status === 'queued' ? '⏳' : 
                            job.status === 'active' ? '🏃 ' : 
                            job.status === 'completed' ? '✅' : 
                            job.status === 'failed' ? '❌' : '•'}
                        </span>
                      </div>
                    </td>
                    <td><span class="truncate" title="${job.job_type || 'default'}">${job.job_type || 'default'}</span></td>
                    <td>${job.agent_name}</td>
                    <td>${this.formatDate(job.created_at)}</td>
                    <td><span class="truncate" title="${job.instructions}">${job.instructions}</span></td>
                    <td>
                      ${html`
                        <span class="clickable" @click=${() => this.showJobResult(job.id)}>View Result</span>
                      <a href="/session/${job.agent_name}/${job.id}" class="job-link" target="_blank">View Session</a>
                      `}
                    </td>
                  </tr>
                `)}
              </tbody>
            </table>
          </div>
        `}
      </div>
    `;
  }
}

customElements.define('jobs-list', JobsList);
