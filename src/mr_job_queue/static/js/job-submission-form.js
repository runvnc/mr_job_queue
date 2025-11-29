import { BaseEl } from '/chat/static/js/base.js';
import { html, css } from '/chat/static/js/lit-core.min.js';

class JobSubmissionForm extends BaseEl {
  static properties = {
    instructions: { type: String },
    selectedAgent: { type: String },
    agents: { type: Array },
    jobType: { type: String },
    selectedModel: { type: String },
    files: { type: Array },
    loading: { type: Boolean },
    error: { type: String },
    success: { type: String }
  };

  static styles = css`
    :host {
      display: block;
      margin-bottom: 2rem;
      width: 100%;
    }

    form {
      background: var(--background-secondary, #2d3748);
      padding: 1.5rem;
      border-radius: 8px;
      box-shadow: 0 2px 4px rgba(0, 0, 0, 0.3);
      width: 100%;
      box-sizing: border-box;
    }

    .form-group {
      margin-bottom: 1.5rem;
    }

    label {
      display: block;
      margin-bottom: 0.75rem;
      font-weight: 500;
      color: var(--text-color, #f7fafc);
      font-size: 1rem;
    }

    textarea, select {
      width: 100%;
      padding: 0.75rem;
      border: 1px solid var(--border-color, #4a5568);
      border-radius: 4px;
      background-color: var(--input-bg, #1a202c);
      color: var(--text-color, #f7fafc);
      font-size: 1rem;
      box-sizing: border-box;
    }

    textarea {
      min-height: 120px;
      resize: vertical;
    }

    button {
      background-color: var(--primary, #667eea);
      color: white;
      border: none;
      padding: 0.75rem 1.5rem;
      border-radius: 4px;
      font-weight: 500;
      cursor: pointer;
      transition: background-color 0.2s;
      font-size: 1rem;
    }

    button:hover {
      background-color: var(--primary-dark, #5a67d8);
    }

    button:disabled {
      background-color: var(--disabled, #718096);
      cursor: not-allowed;
    }

    .error-message {
      color: var(--error, #f56565);
      margin-top: 1rem;
      padding: 0.75rem;
      background-color: var(--error-bg, #742a2a);
      border-radius: 4px;
      border-left: 3px solid var(--error, #f56565);
    }

    .success-message {
      color: var(--success, #48bb78);
      margin-top: 1rem;
      padding: 0.75rem;
      background-color: var(--success-bg, #1c4532);
      border-radius: 4px;
      border-left: 3px solid var(--success, #48bb78);
    }

    .file-list {
      margin-top: 0.5rem;
    }

    .file-item {
      display: flex;
      align-items: center;
      margin-bottom: 0.5rem;
      padding: 0.5rem;
      background-color: var(--file-bg, rgba(255, 255, 255, 0.05));
      border-radius: 4px;
    }

    .file-name {
      flex-grow: 1;
      margin-right: 0.5rem;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }

    /* Custom file upload styling */
    .file-upload-container {
      position: relative;
      display: flex;
      flex-direction: column;
      align-items: center;
      padding: 1.5rem;
      border: 2px dashed var(--border-color, #4a5568);
      border-radius: 8px;
      background-color: rgba(255, 255, 255, 0.03);
      transition: all 0.3s ease;
      cursor: pointer;
      margin-bottom: 1rem;
    }

    .file-upload-container:hover {
      border-color: var(--primary, #667eea);
      background-color: rgba(255, 255, 255, 0.05);
    }

    .file-upload-icon {
      font-size: 2.5rem;
      margin-bottom: 0.5rem;
      color: var(--text-secondary, #cbd5e0);
    }

    .file-upload-text {
      font-size: 1rem;
      color: var(--text-secondary, #cbd5e0);
      margin-bottom: 0.5rem;
      text-align: center;
    }

    .file-upload-subtext {
      font-size: 0.8rem;
      color: var(--text-tertiary, #a0aec0);
      text-align: center;
    }

    /* Hide the actual file input */
    .file-upload-input {
      position: absolute;
      top: 0;
      left: 0;
      width: 100%;
      height: 100%;
      opacity: 0;
      cursor: pointer;
    }

    .remove-file {
      background: none;
      border: none;
      color: var(--error, #f56565);
      cursor: pointer;
      padding: 0.25rem 0.5rem;
    }

    .file-type-icon {
      margin-right: 0.5rem;
      font-size: 1.2rem;
    }

    .loading-spinner {
      display: inline-block;
      width: 1rem;
      height: 1rem;
      border: 2px solid rgba(255, 255, 255, 0.3);
      border-radius: 50%;
      border-top-color: white;
      animation: spin 1s ease-in-out infinite;
      margin-right: 0.5rem;
    }

    @keyframes spin {
      to { transform: rotate(360deg); }
    }

    /* Center submit button */
    .submit-container {
      display: flex;
      justify-content: center;
      margin-top: 1.5rem;
    }

    .submit-container button {
      min-width: 150px;
    }

    /* Add drag and drop support */
    .file-upload-container.dragover {
      border-color: var(--primary, #667eea);
      background-color: rgba(102, 126, 234, 0.1);
      transform: scale(1.02);
    }

  `;

  constructor() {
    super();
    this.instructions = '';
    this.selectedAgent = '';
    this.jobType = 'default';
    this.agents = [];
    this.selectedModel = '';
    this.files = [];
    this.loading = false;
    this.error = '';
    this.success = '';
    this.fetchAgents();
    console.log('JobSubmissionForm initialized');
  }

  async fetchAgents() {
    try {
      // Fetch the list of available agents
      const response = await fetch('/agents/local');
      if (response.ok) {
        const data = await response.json();
        console.log(data)
        this.agents = data || [];
        console.log('Fetched agents:', this.agents);
        if (this.agents.length > 0) {
          this.selectedAgent = this.agents[0];
        }
      } else {
        console.error('Failed to fetch agents:', response.statusText);
      }
    } catch (error) {
      console.error('Error fetching agents:', error);
    }
  }

  handleInstructionsChange(e) {
    this.instructions = e.target.value;
  }

  handleAgentChange(e) {
    this.selectedAgent = e.target.value;
  }

  handleJobTypeChange(e) {
    this.jobType = e.target.value;
  }

  handleModelSelect(e) {
    console.log('Model selected:', e.detail.value);
    this.selectedModel = e.detail.value;
  }

  handleFileChange(e) {
    const fileList = e.target.files;
    if (fileList.length > 0) {
      this.files = [...this.files, ...Array.from(fileList)];
      // Reset the file input
      e.target.value = '';
    }
  }

  removeFile(index) {
    this.files = this.files.filter((_, i) => i !== index);
  }

  handleDrop(e) {
    const dt = e.dataTransfer;
    const files = dt.files;
    
    if (files.length > 0) {
      // Add the dropped files to our files array
      this.files = [...this.files, ...Array.from(files)];
      
      // Update UI
      this.requestUpdate();
    }
  }

  setupDragAndDrop() {
    const dropZone = this.shadowRoot.querySelector('.file-upload-container');
    if (!dropZone) return;

    // Prevent default drag behaviors
    ['dragenter', 'dragover', 'dragleave', 'drop'].forEach(eventName => {
      dropZone.addEventListener(eventName, e => {
        e.preventDefault();
        e.stopPropagation();
      }, false);
    });

    // Visual feedback for drag events
    ['dragenter', 'dragover'].forEach(eventName => {
      dropZone.addEventListener(eventName, () => dropZone.classList.add('dragover'), false);
    });
    ['dragleave', 'drop'].forEach(eventName => {
      dropZone.addEventListener(eventName, () => dropZone.classList.remove('dragover'), false);
    });
    
    // Handle actual file drop
    dropZone.addEventListener('drop', e => {
      this.handleDrop(e);
    }, false);
  }

  async handleSubmit(e) {
    e.preventDefault();
    this.error = '';
    this.success = '';

    if (!this.instructions.trim()) {
      this.error = 'Please enter instructions for the job';
      return;
    }

    if (!this.selectedAgent) {
      this.error = 'Please select an agent';
      return;
    }

    this.loading = true;

    try {
      // Create form data for file uploads
      const formData = new FormData();
      formData.append('instructions', this.instructions);
      
      // Append LLM to job_type if selected
      let finalJobType = this.jobType;
      if (this.selectedModel) {
        finalJobType = `${this.jobType}.${this.selectedModel}`;
      }
      formData.append('job_type', finalJobType);
      formData.append('agent', this.selectedAgent);
      
      if (this.selectedModel) {
        formData.append('metadata', JSON.stringify({ model: this.selectedModel }));
      }

      // Add files if any
      this.files.forEach(file => {
        formData.append('files', file);
      });

      const response = await fetch('/api/jobs', {
        method: 'POST',
        body: formData
      });

      const result = await response.json();

      if (response.ok) {
        this.success = `Job submitted successfully! Job ID: ${result.job_id}`;
        // Reset form
        this.instructions = '';
        this.files = [];
        // Dispatch event to refresh jobs list
        this.dispatch('job-submitted', { jobId: result.job_id });
      } else {
        this.error = result.error || 'Failed to submit job';
      }
    } catch (error) {
      console.error('Error submitting job:', error);
      this.error = 'An error occurred while submitting the job';
    } finally {
      this.loading = false;
    }
  }

  firstUpdated() {
    // Setup drag and drop after the component is first rendered
    setTimeout(() => {
      this.setupDragAndDrop();
    }, 0);
  }

  _render() {
    return html`
      <form @submit=${this.handleSubmit}>
        <div class="form-group">
          <label for="instructions">Job Instructions</label>
          <textarea 
            id="instructions" 
            .value=${this.instructions} 
            @input=${this.handleInstructionsChange} 
            placeholder="Enter detailed instructions for the agent..."
            ?disabled=${this.loading}
          ></textarea>
        </div>

        <div class="form-group">
          <label for="agent">Select Agent</label>
          <select 
            id="agent" 
            .value=${this.selectedAgent} 
            @change=${this.handleAgentChange}
            ?disabled=${this.loading}
          >
            ${this.agents.length === 0 ? html`
              <option value="">Loading agents...</option>
            ` : this.agents.map(agent => html`
              <option value=${agent.name}>${agent.name}</option>
            `)}
          </select>
        </div>

        <div class="form-group">
          <label for="job-type">Job Type</label>
          <input 
            type="text" 
            id="job-type" 
            .value=${this.jobType} 
            @input=${this.handleJobTypeChange}
            placeholder="default"
            ?disabled=${this.loading}
            class="filter-select"
          >
        </div>

        <div class="form-group">
          <llm-selector 
            @llm-selected=${this.handleModelSelect}
          ></llm-selector>
        </div>

        <div class="form-group">
          <label for="files">Upload Files (Optional)</label>
          <div class="file-upload-container" title="Files will be saved to data/uploads/ and made available to the agent">
            <div class="file-upload-icon" style="font-size: 2.5rem;">📂</div>
            <div class="file-upload-text">Drag & drop files here</div>
            <div class="file-upload-subtext">or click to browse files</div>
            <input 
              type="file" 
              id="files" 
              class="file-upload-input"
              @change=${this.handleFileChange} 
              multiple 
              ?disabled=${this.loading}
            >
          </div>

          ${this.files.length > 0 ? html`
            <div class="file-list">
              ${this.files.map((file, index) => html`
                <div class="file-item">
                  <span class="file-name"><span class="file-type-icon">📄</span>${file.name}</span>
                  <button 
                    type="button" 
                    class="remove-file" 
                    @click=${() => this.removeFile(index)}
                    ?disabled=${this.loading}
                  >✕</button>
                </div>
              `)}
            </div>
          ` : ''}
        </div>

        <div class="submit-container">
          <button type="submit" ?disabled=${this.loading}>
            ${this.loading ? html`
              <span class="loading-spinner"></span> Submitting...
            ` : 'Submit Job'}
          </button>
        </div>

        ${this.error ? html`
          <div class="error-message">${this.error}</div>
        ` : ''}

        ${this.files.length > 0 ? html`
          <div class="file-upload-subtext" style="margin-top: 10px; text-align: center; color: var(--text-secondary, #cbd5e0);">Uploaded files will be saved to <code>data/uploads/</code> and included in the job metadata as <code>metadata.uploaded_files</code>.</div>
        ` : ''}

        ${this.success ? html`
          <div class="success-message">${this.success}</div>
        ` : ''}
      </form>
    `;
  }
}

customElements.define('job-submission-form', JobSubmissionForm);
