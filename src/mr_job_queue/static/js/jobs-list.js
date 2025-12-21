import { BaseEl } from '/chat/static/js/base.js';
import { html, css } from '/chat/static/js/lit-core.min.js';

class TokenTreeNode extends BaseEl {
  static properties = {
    nodeData: { type: Object },
    expanded: { type: Boolean },
    isRoot: { type: Boolean }
  };

  static styles = css`
    .tree-node {
      margin: 0.5rem 0;
      border-left: 2px solid var(--border-color, #4a5568);
      padding-left: 1rem;
      position: relative;
    }

    .tree-node:before {
      content: '';
      position: absolute;
      left: -2px;
      top: 1rem;
      width: 12px;
      height: 2px;
      background: var(--border-color, #4a5568);
    }

    .tree-node.root {
      border-left: none;
      padding-left: 0;
    }

    .tree-node.root:before {
      display: none;
    }

    .node-header {
      display: flex;
      align-items: center;
      gap: 0.5rem;
      margin-bottom: 0.5rem;
      padding: 0.5rem;
      background: rgba(255, 255, 255, 0.05);
      border-radius: 4px;
      cursor: pointer;
      transition: background-color 0.2s;
    }

    .node-header:hover {
      background: rgba(255, 255, 255, 0.1);
    }

    .expand-icon {
      font-size: 0.8rem;
      color: var(--text-secondary, #cbd5e0);
      width: 12px;
      text-align: center;
    }

    .node-info {
      flex: 1;
      display: flex;
      align-items: center;
      gap: 1rem;
    }

    .node-id {
      font-weight: bold;
      color: var(--primary, #667eea);
      text-decoration: none;
    }

    .node-id:hover {
      text-decoration: underline;
    }

    .node-agent {
      color: var(--text-secondary, #cbd5e0);
      font-size: 0.85rem;
    }

    .token-counts {
      display: flex;
      gap: 0.5rem;
      font-size: 0.8rem;
    }

    .token-count {
      display: flex;
      flex-direction: column;
      align-items: center;
      min-width: 50px;
    }

    .token-label {
      color: var(--text-secondary, #cbd5e0);
      font-size: 0.7rem;
      margin-bottom: 0.2rem;
    }

    .token-value {
      color: var(--text-color, #f7fafc);
      font-weight: bold;
    }

    .token-count.individual .token-value {
      color: var(--success, #48bb78);
    }

    .token-count.cumulative .token-value {
      color: var(--warning, #ed8936);
    }

    .node-children {
      margin-left: 1rem;
      display: none;
    }

    .node-children.expanded {
      display: block;
    }
  `;

  constructor() {
    super();
    this.expanded = false;
    this.isRoot = false;
  }

  toggleExpanded() {
    this.expanded = !this.expanded;
    this.requestUpdate();
  }

  formatNumber(num) {
    return num ? num.toLocaleString() : '0';
  }

  _render() {
    if (!this.nodeData) return html``;

    const hasChildren = this.nodeData.children && this.nodeData.children.length > 0;
    const expandIcon = hasChildren ? (this.expanded ? '▼' : '▶') : '•';

    return html`
      <div class="tree-node ${this.isRoot ? 'root' : ''}">
        <div class="node-header" @click=${this.toggleExpanded}>
          <span class="expand-icon">${expandIcon}</span>
          <div class="node-info">
            <a href="/session/${this.nodeData.agent}/${this.nodeData.log_id}" 
               target="_blank" class="node-id">
              ${this.nodeData.log_id}
            </a>
            <span class="node-agent">(${this.nodeData.agent})</span>
          </div>
          <div class="token-counts">
            <div class="token-count individual">
              <div class="token-label">Input (Individual)</div>
              <div class="token-value">
                ${this.formatNumber(this.nodeData.individual_counts.input_tokens_sequence)}
              </div>
            </div>
            <div class="token-count individual">
              <div class="token-label">Output (Individual)</div>
              <div class="token-value">
                ${this.formatNumber(this.nodeData.individual_counts.output_tokens_sequence)}
              </div>
            </div>
            <div class="token-count cumulative">
              <div class="token-label">Input (Cumulative)</div>
              <div class="token-value">
                ${this.formatNumber(this.nodeData.cumulative_counts.input_tokens_sequence)}
              </div>
            </div>
            <div class="token-count cumulative">
              <div class="token-label">Output (Cumulative)</div>
              <div class="token-value">
                ${this.formatNumber(this.nodeData.cumulative_counts.output_tokens_sequence)}
              </div>
            </div>
            <div class="token-count">
              <div class="token-label">Context Total</div>
              <div class="token-value">
                ${this.formatNumber(this.nodeData.cumulative_counts.input_tokens_total)}
              </div>
            </div>
          </div>
        </div>
        ${hasChildren && this.expanded ? html`
          <div class="node-children expanded">
            ${this.nodeData.children.map(child => html`
              <token-tree-node .nodeData=${child}></token-tree-node>
            `)}
          </div>
        ` : ''}
      </div>
    `;
  }
}

customElements.define('token-tree-node', TokenTreeNode);

class TokenTree extends BaseEl {
  static properties = {
    hierarchy: { type: Object },
    allExpanded: { type: Boolean }
  };

  static styles = css`
    :host {
      display: block;
      font-family: monospace;
      font-size: 0.9rem;
      line-height: 1.4;
    }

    .token-summary {
      background: var(--background-primary, #1a202c);
      padding: 1rem;
      border-radius: 6px;
      margin-bottom: 1rem;
      border: 1px solid var(--border-color, #4a5568);
    }

    .summary-title {
      font-size: 1rem;
      font-weight: 600;
      color: var(--text-color, #f7fafc);
      margin-bottom: 0.5rem;
    }

    .summary-stats {
      display: flex;
      gap: 1rem;
      flex-wrap: wrap;
    }

    .summary-stat {
      display: flex;
      flex-direction: column;
      align-items: center;
      min-width: 70px;
    }

    .summary-label {
      color: var(--text-secondary, #cbd5e0);
      font-size: 0.75rem;
      margin-bottom: 0.25rem;
    }

    .summary-value {
      color: var(--text-color, #f7fafc);
      font-weight: bold;
      font-family: monospace;
      font-size: 1.1em;
    }

    .tree-controls {
      margin-bottom: 1rem;
      display: flex;
      gap: 0.5rem;
      align-items: center;
    }

    .expand-all-btn, .collapse-all-btn {
      background-color: var(--secondary, #a0aec0);
      color: white;
      border: none;
      padding: 0.25rem 0.5rem;
      border-radius: 4px;
      cursor: pointer;
      font-size: 0.75rem;
      margin-right: 0.5rem;
      transition: background-color 0.2s;
    }

    .expand-all-btn:hover, .collapse-all-btn:hover {
      background-color: var(--secondary-dark, #718096);
    }

    .tree-legend {
      display: flex;
      gap: 2rem;
      margin-bottom: 1rem;
      padding: 0.5rem;
      background: rgba(255, 255, 255, 0.05);
      border-radius: 4px;
      font-size: 0.8rem;
    }

    .legend-item {
      display: flex;
      align-items: center;
      gap: 0.5rem;
    }

    .legend-color {
      width: 12px;
      height: 12px;
      border-radius: 2px;
    }

    .legend-individual {
      background: var(--success, #48bb78);
    }

    .legend-cumulative {
      background: var(--warning, #ed8936);
    }

    .error-message {
      color: var(--error, #f56565);
      padding: 0.75rem;
      background-color: var(--error-bg, #742a2a);
      border-radius: 4px;
      border-left: 3px solid var(--error, #f56565);
      margin-bottom: 1rem;
    }
  `;

  constructor() {
    super();
    this.allExpanded = false;
  }

  expandAll() {
    this.allExpanded = true;
    this.expandAllNodes(true);
  }

  collapseAll() {
    this.allExpanded = false;
    this.expandAllNodes(false);
  }

  expandAllNodes(expanded) {
    const nodes = this.shadowRoot.querySelectorAll('token-tree-node');
    nodes.forEach(node => {
      node.expanded = expanded;
      node.requestUpdate();
    });
  }

  formatNumber(num) {
    return num ? num.toLocaleString() : '0';
  }

  _render() {
    if (!this.hierarchy) {
      return html`<div class="error-message">No hierarchy data available</div>`;
    }

    const mainInput = this.hierarchy.individual_counts.input_tokens_sequence;
    const mainOutput = this.hierarchy.individual_counts.output_tokens_sequence;
    const totalInput = this.hierarchy.cumulative_counts.input_tokens_sequence;
    const totalOutput = this.hierarchy.cumulative_counts.output_tokens_sequence;
    const delegatedInput = totalInput - mainInput;
    const delegatedOutput = totalOutput - mainOutput;

    return html`
      <div class="token-tree">
        <div class="token-summary">
          <div class="summary-title">Token Usage Summary</div>
          <div class="summary-stats">
            <div class="summary-stat">
              <div class="summary-label">Main Input</div>
              <div class="summary-value">${this.formatNumber(mainInput)}</div>
            </div>
            <div class="summary-stat">
              <div class="summary-label">Main Output</div>
              <div class="summary-value">${this.formatNumber(mainOutput)}</div>
            </div>
            <div class="summary-stat">
              <div class="summary-label">Total Input</div>
              <div class="summary-value">${this.formatNumber(totalInput)}</div>
            </div>
            <div class="summary-stat">
              <div class="summary-label">Total Output</div>
              <div class="summary-value">${this.formatNumber(totalOutput)}</div>
            </div>
            <div class="summary-stat">
              <div class="summary-label">Delegated Input</div>
              <div class="summary-value">${this.formatNumber(delegatedInput)}</div>
            </div>
            <div class="summary-stat">
              <div class="summary-label">Delegated Output</div>
              <div class="summary-value">${this.formatNumber(delegatedOutput)}</div>
            </div>
            <div class="summary-stat">
              <div class="summary-label">Context Total</div>
              <div class="summary-value">${this.formatNumber(this.hierarchy.cumulative_counts.input_tokens_total)}</div>
            </div>
          </div>
        </div>

        <div class="tree-controls">
          <button class="expand-all-btn" @click=${this.expandAll}>Expand All</button>
          <button class="collapse-all-btn" @click=${this.collapseAll}>Collapse All</button>
        </div>

        <div class="tree-legend">
          <div class="legend-item">
            <div class="legend-color legend-individual"></div>
            <span>Individual: Input/Output tokens used by this specific task only</span>
          </div>
          <div class="legend-item">
            <div class="legend-color legend-cumulative"></div>
            <span>Cumulative: Input/Output tokens including all subtasks in this branch</span>
          </div>
        </div>

        <token-tree-node .nodeData=${this.hierarchy} .isRoot=${true} .expanded=${true}></token-tree-node>
      </div>
    `;
  }
}

customElements.define('token-tree', TokenTree);

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
      overflow: hidden;
      text-overflow: ellipsis;
      display: inline-block;
    }

    .col-id { width: 12%; }
    .col-status { width: 5%; }
    .col-job-type { width: 15%; max-width: 150px; }
    .col-agent { width: 10%; }
    .col-created { width: 12%; }
    .col-instructions { width: 28%; }
    .col-result { width: 10%; }
    .col-tokens { width: 8%; }

    .token-btn {
      background-color: var(--info, #63b3ed);
      color: white;
      border: none;
      padding: 0.25rem 0.5rem;
      border-radius: 4px;
      cursor: pointer;
      font-size: 0.75rem;
      transition: background-color 0.2s;
    }

    .token-btn:hover {
      background-color: var(--info-dark, #3182ce);
    }

    .print-btn {
      background-color: var(--primary, #667eea);
      color: white;
      border: none;
      padding: 0.5rem 1rem;
      border-radius: 4px;
      cursor: pointer;
      font-size: 0.9rem;
      margin-right: 1rem;
    }

    .modal-content {
      background-color: var(--background-secondary, #2d3748);
      margin: auto;
      padding: 20px;
      border-radius: 8px;
      border: none;
      max-width: 1400px;
      width: 90%;
      max-height: 90vh;
      overflow-y: auto;
    }

    dialog.modal-content {
      position: fixed;
      top: 50%;
      left: 50%;
      transform: translate(-50%, -50%);
    }

    dialog.modal-content::backdrop {
      background-color: rgba(0, 0, 0, 0.7);
    }

    .modal-content {
      width: 90%;
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

    .modal-header {
      display: flex;
      justify-content: space-between;
    }

    .markdown-result h1, .markdown-result h2, .markdown-result h3 { 
      margin-top: 1rem; 
      margin-bottom: 0.5rem; 
    }

    .markdown-result p { 
      margin-bottom: 1rem; 
    }

    .markdown-result pre { 
      background: rgba(0,0,0,0.2); 
      padding: 0.5rem; 
      border-radius: 4px; 
      overflow-x: auto; 
    }

    .markdown-result code { 
      font-family: monospace; 
      background: rgba(0,0,0,0.2); 
      padding: 0.1rem 0.3rem; 
      border-radius: 3px; 
    }

    .markdown-result ul, .markdown-result ol { 
      margin-left: 1.5rem; 
      margin-bottom: 1rem; 
    }

    .markdown-result img { 
      max-width: 100%; 
      height: auto; 
    }

    .markdown-result blockquote { 
      border-left: 3px solid var(--border-color, #4a5568); 
      padding-left: 1rem; 
      margin-left: 0; 
      color: var(--text-secondary, #cbd5e0); 
    }
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

    // Initialize modal elements
    this.resultModal = null;
    this.tokenModal = null;
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

  printJobResult() {
    const modalContent = this.resultModal.querySelector('.result-content').innerHTML;
    const printWindow = window.open('', '', 'height=600,width=800');
    printWindow.document.write('<html><head><title>Job Result</title>');
    printWindow.document.write('<style>');
    printWindow.document.write('body { font-family: sans-serif; } pre { white-space: pre-wrap; }');
    printWindow.document.write('</style>');
    printWindow.document.write('</head><body>');
    printWindow.document.write(modalContent);
    printWindow.document.write('</body></html>');
    printWindow.document.close();
    printWindow.focus();
    printWindow.print();
    printWindow.close();
  }

  async showJobResult(jobId) {
    try {
      const modalId = "job-output";
      
      // Ensure global styles for dialogs are loaded
      if (!document.getElementById('dialog-styles')) {
        const style = document.createElement('style');
        style.id = 'dialog-styles';
        style.textContent = `
          dialog.modal-content {
            position: fixed;
            top: 50%;
            left: 50%;
            transform: translate(-50%, -50%);
            margin: 0;
          }
        `;
        document.head.appendChild(style);
      }
      
      if (!this.resultModal) {
        this.resultModal = document.getElementById(modalId);
      }

      if (!this.resultModal) {
        this.resultModal = document.createElement('dialog');
        this.resultModal.id = modalId;
        document.body.appendChild(this.resultModal);
        this.resultModal.addEventListener('click', (e) => {
          if (e.target === this.resultModal) {
            this.resultModal.close();
          }
        });
        this.resultModal.addEventListener('cancel', (e) => {
          if (e.target === this.resultModal) {
            this.resultModal.close();
          }
        });
      }

      this.resultModal.className = 'modal-content';
      this.resultModal.innerHTML = `
        <div class="modal-header">
          <h3>Job Result</h3>
          <div>
            <button class="print-btn">Print</button>
            <button class="close-modal">&times;</button>
          </div>
        </div>
        <div class="result-content"></div>
      `;

      this.resultModal.querySelector('.close-modal').addEventListener('click', () => this.resultModal.close());
      this.resultModal.querySelector('.print-btn').addEventListener('click', () => this.printJobResult());

      const response = await fetch(`/api/jobs/${jobId}`);
      if (response.ok) {
        const jobData = await response.json();
        this.ensureMarkedJsLoaded();
        const result = jobData.result || 'No result available';
        const resultContent = this.resultModal.querySelector('.result-content');
        const isMarkdown = this.looksLikeMarkdown(result);

        if (isMarkdown) {
          try {
            if (window.marked) {
              resultContent.innerHTML = '';
              const markdownDiv = document.createElement('div');
              markdownDiv.className = 'markdown-result';
              markdownDiv.innerHTML = marked.parse(result);
              resultContent.appendChild(markdownDiv);
            } else {
              resultContent.innerHTML = result;
              console.warn('Marked.js not available for markdown rendering');
            }
          } catch (error) {
            console.error('Error parsing markdown:', error);
            resultContent.innerHTML = result;
          }
        } else {
          resultContent.innerHTML = result;
        }

        // Prevent page scroll when showing modal
        const scrollY = window.scrollY;
        this.resultModal.showModal();
        requestAnimationFrame(() => {
          window.scrollTo(0, scrollY);
        });
      } else {
        console.error(`Error fetching job result for ${jobId}:`, await response.json());
      }
    } catch (error) {
      console.error('Error showing job result:', error);
    }
  }

  ensureMarkedJsLoaded() {
    if (window.marked) return;
    
    if (document.querySelector('script[src*="marked"]')) return;
    
    const script = document.createElement('script');
    script.src = 'https://cdn.jsdelivr.net/npm/marked/marked.min.js';
    script.async = true;
    script.onload = () => {
      console.log('Marked.js loaded successfully');
      if (window.marked) {
        marked.setOptions({ breaks: true });
      }
    };
    document.head.appendChild(script);
  }

  looksLikeMarkdown(text) {
    if (!text || typeof text !== 'string') return false;
    
    const markdownPatterns = [
      /^#+ .*$/m,
      /\*\*.*\*\*/,
      /\*.*\*/,
      /\[.*\]\(.*\)/,
      /^- .*$/m,
      /^\d+\. .*$/m,
      /^```[\s\S]*?```$/m,
      /^>.*$/m
    ];
    
    return markdownPatterns.some(pattern => pattern.test(text));
  }

  async showTokenCounts(jobId) {
    try {
      const modalId = "token-counts";
      if (!this.tokenModal) {
        this.tokenModal = document.getElementById(modalId);
      }

      if (!this.tokenModal) {
        this.tokenModal = document.createElement('dialog');
        this.tokenModal.id = modalId;
        document.body.appendChild(this.tokenModal);
        this.tokenModal.addEventListener('click', (e) => {
          if (e.target === this.tokenModal) {
            this.tokenModal.close();
          }
        });
        this.tokenModal.addEventListener('cancel', (e) => {
          if (e.target === this.tokenModal) {
            this.tokenModal.close();
          }
        });
      }

      this.tokenModal.className = 'modal-content';
      this.tokenModal.innerHTML = `
        <div class="modal-header">
          <h3>Token Analysis - Job ${jobId}</h3>
          <button class="close-modal">&times;</button>
        </div>
        <div class="token-content">
          <div class="loading-indicator">
            <div class="spinner"></div>
            <p>Loading token analysis...</p>
          </div>
        </div>
      `;

      this.tokenModal.querySelector('.close-modal').addEventListener('click', () => this.tokenModal.close());
      
      // Prevent page scroll when showing modal
      const scrollY = window.scrollY;
      this.tokenModal.showModal();
      requestAnimationFrame(() => {
        window.scrollTo(0, scrollY);
      });

      // Fetch token data
      const response = await fetch(`/api/jobs/${jobId}/tokens`);
      const tokenContent = this.tokenModal.querySelector('.token-content');
      
      if (response.ok) {
        const data = await response.json();
        const hierarchy = data.hierarchy;
        
        if (hierarchy) {
          // Create and insert the token tree component
          const tokenTree = document.createElement('token-tree');
          tokenTree.hierarchy = hierarchy;
          tokenContent.innerHTML = '';
          tokenContent.appendChild(tokenTree);
        } else {
          tokenContent.innerHTML = '<div class="error-message">No hierarchical token data available</div>';
        }
      } else {
        const errorData = await response.json();
        tokenContent.innerHTML = `<div class="error-message">Error loading token data: ${errorData.error}</div>`;
      }
    } catch (error) {
      console.error('Error showing token counts:', error);
    }
  }

  async cancelJob(jobId) {
    if (!confirm(`Are you sure you want to cancel job ${jobId}?`)) return;
    
    try {
      const response = await fetch(`/api/jobs/${jobId}`, {
        method: 'DELETE'
      });
      if (response.ok) {
        this.loadJobs();
      } else {
        const errorData = await response.json();
        alert(`Error cancelling job: ${errorData.error}`);
      }
    } catch (error) {
      console.error('Error cancelling job:', error);
    }
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
                  <th class="col-tokens">Tokens</th>
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
                      <span class="clickable" @click=${() => this.showJobResult(job.id)}>View Result</span>
                      ${['queued', 'active'].includes(job.status) ? html`
                        <span class="clickable" style="color: var(--error, #f56565); margin-left: 10px;" @click=${() => this.cancelJob(job.id)}>
                          Cancel
                        </span>
                      ` : ''}
                      <a href="/session/${job.agent_name}/${job.id}" class="job-link" target="_blank">View Session</a>
                    </td>
                    <td>
                      <button class="token-btn" @click=${() => this.showTokenCounts(job.id)}>Token Tree</button>
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
