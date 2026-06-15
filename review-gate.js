/* ═══════════════════════════════════════════════════════════════
   ZEN CHAT REVIEW GATE SYSTEM
   Completion popup, Accept/Alter/Review flows, GitHub update plan,
   Change method, Failure diagnosis
   ═══════════════════════════════════════════════════════════════ */

// ── State ──────────────────────────────────────────────────────
const ZEN_REVIEW = {
  // Current open review popup state
  active: false,
  workId: null,
  view: null,        // 'full' | 'drawer' | 'small'
  sessionId: null,
  prompt: '',        // original user prompt
  response: '',      // Hermes response
  progress: [],      // tool progress lines
  plan: null,        // GitHub update plan (set during execution)
  evidence: [],      // files touched, checks run
  diagnosis: null,   // failure diagnosis (set on error)

  // Review type state
  reviewType: null,  // 'backend' | 'frontend' | 'fullstack'

  // Previous page state for frontend review comparison
  beforeSnapshot: null,  // DOM/capture before patch
};

// ── GitHub Update Plan Builder ─────────────────────────────────
function zenReviewBuildPlan(opts) {
  // opts: { filesTouched, checksRun, checksPassed, checksFailed, hasChanges, PRTarget }
  const plan = {
    summary: '',
    steps: [],
    method: opts.method || 'commit-push',
  };

  if (!opts.hasChanges || !opts.filesTouched || !opts.filesTouched.length) {
    plan.summary = 'No file changes detected. No GitHub update needed.';
    plan.steps = [{ icon: 'ℹ️', text: 'No files were modified.' }];
    return plan;
  }

  const files = opts.filesTouched;
  const fileList = files.join(', ');
  plan.summary = `Update ${files.length} file${files.length > 1 ? 's' : ''}: ${fileList}`;

  // Step 1: Commit
  plan.steps.push({ icon: '📝', text: `git add ${fileList}` });
  plan.steps.push({ icon: '✅', text: `git commit -m "<message>"` });

  // Step 2: Push or PR
  if (opts.method === 'pr') {
    plan.steps.push({ icon: '🌿', text: `git push -u origin <branch>` });
    plan.steps.push({ icon: '🔀', text: `Open PR → ${opts.PRTarget || 'main'}` });
  } else if (opts.method === 'comment') {
    plan.steps = [{ icon: '💬', text: 'Add comment only — no commit or push.' }];
    plan.summary = 'Add comment to existing task/PR. No file changes.';
  } else {
    plan.steps.push({ icon: '⬆️', text: `git push` });
  }

  // Step 3: Update task/summary
  if (opts.updateTask) {
    plan.steps.push({ icon: '📋', text: 'Update Kanban task summary + evidence.' });
  }

  // Step 4: Validation status
  if (opts.checksRun && opts.checksRun.length) {
    const passed = (opts.checksPassed || []).length;
    const failed = (opts.checksFailed || []).length;
    const total = opts.checksRun.length;
    if (failed > 0) {
      plan.steps.push({ icon: '⚠️', text: `${passed}/${total} checks passed. ${failed} failed — review required.` });
    } else {
      plan.steps.push({ icon: '✅', text: `${passed}/${total} checks passed.` });
    }
  }

  return plan;
}

// ── Show Completion Popup ──────────────────────────────────────
function zenReviewShow(opts) {
  // opts: { workId, view, sessionId, prompt, response, plan, evidence, progress, status: 'complete'|'failed' }
  ZEN_REVIEW.active = true;
  ZEN_REVIEW.workId = opts.workId;
  ZEN_REVIEW.view = opts.view;
  ZEN_REVIEW.sessionId = opts.sessionId;
  ZEN_REVIEW.prompt = opts.prompt || '';
  ZEN_REVIEW.response = opts.response || '';
  ZEN_REVIEW.progress = opts.progress || [];
  ZEN_REVIEW.evidence = opts.evidence || [];
  ZEN_REVIEW.plan = opts.plan || null;
  ZEN_REVIEW.status = opts.status || 'complete';

  // Block chat input
  zenChatBlockInput(opts.view);

  // Render popup into the correct view container
  const container = zenReviewGetContainer(opts.view);
  if (!container) return;

  const html = opts.status === 'failed'
    ? zenReviewRenderDiagnosis(opts)
    : zenReviewRenderCompletion(opts);

  // Remove any existing popup
  const existing = container.querySelector('.zen-review-popup');
  if (existing) existing.remove();

  const popup = document.createElement('div');
  popup.className = 'zen-review-popup';
  popup.id = 'zen-review-popup';
  popup.innerHTML = html;
  container.appendChild(popup);
  zenReviewKeepPopupVisible(popup, opts.view);

  // Wire event listeners
  zenReviewWireListeners(popup, opts);
}

// ── Get container for popup ────────────────────────────────────
function zenReviewGetContainer(view) {
  if (view === 'small') {
    return document.getElementById('zen-chat-small-panel');
  } else if (view === 'drawer') {
    return document.getElementById('zen-chat-drawer');
  }
  // full page — overlay the chat shell, not the scrollback stream
  return document.getElementById('zen-chat-full-page');
}

// ── Render Completion Popup ────────────────────────────────────
function zenReviewRenderCompletion(opts) {
  const plan = opts.plan || { summary: 'Work complete. No file changes detected.', steps: [] };
  const responsePreview = (opts.response || '').substring(0, 300);
  const stepHtml = plan.steps.map(s => `
    <div class="zen-review-plan-step">
      <span class="zen-review-plan-icon">${s.icon}</span>
      <span class="zen-review-plan-text">${zenReviewEscape(s.text)}</span>
    </div>`).join('');

  return `
    <div class="zen-review-backdrop"></div>
    <div class="zen-review-card">
      <div class="zen-review-header">
        <div class="zen-review-status-icon">✅</div>
        <div class="zen-review-header-text">
          <div class="zen-review-title">Execution Complete</div>
          <div class="zen-review-subtitle">Review before GitHub update</div>
        </div>
        <div class="zen-review-risk-badge ${zenReviewRiskClass(opts)}">${zenReviewRiskLabel(opts)}</div>
      </div>

      <div class="zen-review-body">
        <!-- Response preview -->
        <div class="zen-review-section">
          <div class="zen-review-section-label">RESPONSE</div>
          <div class="zen-review-response-preview">${zenReviewEscape(responsePreview)}${(opts.response || '').length > 300 ? '…' : ''}</div>
        </div>

        <!-- GitHub Update Plan -->
        <div class="zen-review-section">
          <div class="zen-review-section-label">GITHUB UPDATE PLAN</div>
          <div class="zen-review-plan-summary">${zenReviewEscape(plan.summary)}</div>
          <div class="zen-review-plan-steps">${stepHtml}</div>
        </div>

        <!-- Evidence -->
        ${opts.evidence && opts.evidence.length ? `
        <div class="zen-review-section">
          <div class="zen-review-section-label">EVIDENCE</div>
          <div class="zen-review-evidence">
            ${opts.evidence.map(e => `<span class="zen-review-evidence-pill">${zenReviewEscape(e)}</span>`).join('')}
          </div>
        </div>` : ''}
      </div>

      <div class="zen-review-actions">
        <button class="zen-review-btn zen-review-btn--accept" data-action="accept">
          <span>✓</span> Accept
        </button>
        <button class="zen-review-btn zen-review-btn--alter" data-action="alter">
          <span>✎</span> Alter
        </button>
        <button class="zen-review-btn zen-review-btn--review" data-action="review">
          <span>👁</span> Review
        </button>
        <button class="zen-review-btn zen-review-btn--change-method" data-action="change-method">
          <span>⚙</span> Change Method
        </button>
      </div>
    </div>`;
}

// ── Render Failure Diagnosis ───────────────────────────────────
function zenReviewRenderDiagnosis(opts) {
  const diagnosis = opts.diagnosis || {};
  const cause = diagnosis.cause || 'Unknown error';
  const evidence = diagnosis.evidence || '';
  const recovery = diagnosis.recovery || ['Retry', 'Alter request', 'Check logs'];

  return `
    <div class="zen-review-backdrop"></div>
    <div class="zen-review-card zen-review-card--failed">
      <div class="zen-review-header">
        <div class="zen-review-status-icon zen-review-status-icon--failed">⚠️</div>
        <div class="zen-review-header-text">
          <div class="zen-review-title">Execution Failed</div>
          <div class="zen-review-subtitle">Diagnosis required before retry</div>
        </div>
        <div class="zen-review-risk-badge zen-review-risk-badge--high">FAILURE</div>
      </div>

      <div class="zen-review-body">
        <!-- Cause -->
        <div class="zen-review-section">
          <div class="zen-review-section-label">LIKELY CAUSE</div>
          <div class="zen-review-diagnosis-cause">${zenReviewEscape(cause)}</div>
        </div>

        <!-- Evidence / Logs -->
        ${evidence ? `
        <div class="zen-review-section">
          <div class="zen-review-section-label">EVIDENCE / LOGS</div>
          <div class="zen-review-diagnosis-evidence">${zenReviewEscape(evidence)}</div>
        </div>` : ''}

        <!-- Recovery options -->
        <div class="zen-review-section">
          <div class="zen-review-section-label">RECOVERY OPTIONS</div>
          <div class="zen-review-recovery">
            ${recovery.map((r, i) => `
              <button class="zen-review-recovery-btn" data-recovery="${i}">${zenReviewEscape(r)}</button>
            `).join('')}
          </div>
        </div>
      </div>

      <div class="zen-review-actions">
        <button class="zen-review-btn zen-review-btn--alter" data-action="alter">
          <span>✎</span> Alter Request
        </button>
        <button class="zen-review-btn zen-review-btn--retry" data-action="retry">
          <span>↻</span> Retry
        </button>
      </div>
    </div>`;
}

// ── Wire popup event listeners ─────────────────────────────────
function zenReviewWireListeners(popup, opts) {
  popup.querySelectorAll('[data-action]').forEach(btn => {
    btn.addEventListener('click', function() {
      const action = this.dataset.action;
      zenReviewHandleAction(action, opts);
    });
  });

  popup.querySelectorAll('[data-recovery]').forEach(btn => {
    btn.addEventListener('click', function() {
      const idx = parseInt(this.dataset.recovery, 10);
      const recovery = opts.diagnosis && opts.diagnosis.recovery ? opts.diagnosis.recovery[idx] : 'Retry';
      zenReviewHandleRecovery(recovery, opts);
    });
  });
}

// ── Handle popup action ────────────────────────────────────────
function zenReviewHandleAction(action, opts) {
  switch (action) {
    case 'accept':
      zenReviewAccept(opts);
      break;
    case 'alter':
      zenReviewAlter(opts);
      break;
    case 'review':
      zenReviewOpenReviewSelector(opts);
      break;
    case 'change-method':
      zenReviewShowChangeMethod(opts);
      break;
    case 'retry':
      zenReviewRetry(opts);
      break;
  }
}

// ── Accept ──────────────────────────────────────────────────────
function zenReviewAccept(opts) {
  const plan = ZEN_REVIEW.plan;
  if (!plan) {
    zenReviewDismiss();
    zenChatUnblockInput(opts.view);
    return;
  }

  zenReviewDismiss();

  const workId = opts.workId;
  const view = opts.view;
  const hasExecutableSteps = plan.steps && plan.steps.some(function(step) {
    return step && step.icon !== 'ℹ️';
  });
  const isNoopPlan = !hasExecutableSteps || /no file changes detected|no github update needed/i.test(plan.summary || '');

  // If the gate says there is nothing to update, approve locally only. This
  // avoids a confusing/risky /api/github-update call for pure chat replies.
  if (isNoopPlan) {
    if (workId && ZEN_WORK_STATE.tasks[workId]) {
      zenWorkAddActivity(workId, '✅', 'accept', 'Accepted — no GitHub update required.', 'done');
      zenWorkTransition(workId, 'complete', { action: 'Approved locally. No GitHub update required.' });
      zenWorkRenderAll();
    }
    zenChatUnblockInput(view);
    zenChatSetStatus('idle', view);
    zenChatUpdateSendBtn(view);
    return;
  }

  // Run the GitHub update plan
  zenWorkAddActivity(workId, '🚀', 'accept', 'Accepted — running GitHub update plan.', 'active');
  zenWorkTransition(workId, 'running', { action: 'Running GitHub update plan.' });

  // Execute plan steps via API
  zenReviewExecutePlan(plan, opts).then(function(result) {
    zenWorkAddActivity(workId, '✅', 'complete', 'GitHub update plan executed.', 'done');
    zenWorkTransition(workId, 'complete', { action: 'GitHub update complete. Task approved.' });
    zenWorkRenderAll();
    zenChatUnblockInput(view);
    zenChatSetStatus('idle', view);
    zenChatUpdateSendBtn(view);
  }).catch(function(err) {
    zenWorkAddActivity(workId, '❌', 'failure', 'GitHub update failed: ' + (err.message || err), 'failed');
    zenWorkTransition(workId, 'failed', {
      action: 'GitHub update failed.',
      cause: err.message || 'GitHub update failed',
      recovery: ['Retry GitHub update', 'Alter plan', 'Manual push']
    });
    zenWorkRenderAll();
    zenChatUnblockInput(view);
    zenChatSetStatus('idle', view);
    zenChatUpdateSendBtn(view);
  });

  // Refresh chat
  zenChatSetStatus('idle', view);
  zenChatUpdateSendBtn(view);
  zenChatRefreshActiveMessages(true);
}

// ── Execute GitHub Update Plan via API ─────────────────────────
function zenReviewExecutePlan(plan, opts) {
  return fetch('/api/github-update', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      plan: plan,
      sessionId: opts.sessionId,
      prompt: opts.prompt,
      evidence: opts.evidence || [],
    })
  }).then(function(resp) {
    if (!resp.ok) throw new Error('GitHub update failed: HTTP ' + resp.status);
    return resp.json();
  });
}

// ── Alter ──────────────────────────────────────────────────────
function zenReviewAlter(opts) {
  zenReviewDismiss();
  // Unlock chat — work stays in context
  zenChatUnblockInput(opts.view);

  const suffix = opts.view === 'small' ? '-small' : (opts.view === 'drawer' ? '-drawer' : '');
  const input = document.getElementById('zen-chat' + suffix + '-input');
  if (input) {
    input.value = 'Alter request for previous result: ';
    input.focus();
    if (input.setSelectionRange) input.setSelectionRange(input.value.length, input.value.length);
    zenChatAutoResize(input);
    zenChatUpdateSendBtn(opts.view);
  }

  // Add a note to the working state that work was altered
  const workId = opts.workId;
  if (workId && ZEN_WORK_STATE.tasks[workId]) {
    zenWorkAddActivity(workId, '✎', 'alter', 'User chose to alter. Drafting follow-up alteration request in chat input.', 'done');
    zenWorkTransition(workId, 'complete', { action: 'Ready for Alter follow-up.' });
    zenWorkRenderAll();
  }
}

// ── Retry ──────────────────────────────────────────────────────
function zenReviewRetry(opts) {
  zenReviewDismiss();
  // Re-send the original prompt
  const view = opts.view;
  const text = ZEN_REVIEW.prompt;

  // Re-use zenChatSend with the stored prompt
  if (text) {
    const suffix = view === 'small' ? '-small' : (view === 'drawer' ? '-drawer' : '');
    const input = document.getElementById('zen-chat' + suffix + '-input');
    if (input) {
      input.value = text;
      zenChatSend(view);
    }
  }
}

// ── Recovery action ────────────────────────────────────────────
function zenReviewHandleRecovery(recovery, opts) {
  if (recovery.toLowerCase().includes('retry')) {
    zenReviewRetry(opts);
  } else if (recovery.toLowerCase().includes('alter')) {
    zenReviewAlter(opts);
  } else {
    // Custom recovery — dismiss and let user type
    zenReviewDismiss();
    zenChatUnblockInput(opts.view);
  }
}

// ── Show Change GitHub Update Method ───────────────────────────
function zenReviewShowChangeMethod(opts) {
  const popup = document.getElementById('zen-review-popup');
  if (!popup) return;

  // Replace popup body with method selector
  const card = popup.querySelector('.zen-review-card');
  if (!card) return;

  // Save current plan for restore on cancel
  ZEN_REVIEW._savedPlan = ZEN_REVIEW.plan;
  ZEN_REVIEW._savedOpts = opts;

  card.innerHTML = `
    <div class="zen-review-header">
      <div class="zen-review-status-icon">⚙️</div>
      <div class="zen-review-header-text">
        <div class="zen-review-title">Change GitHub Update Method</div>
        <div class="zen-review-subtitle">Choose how to update GitHub</div>
      </div>
    </div>
    <div class="zen-review-body">
      <div class="zen-review-method-list">
        <button class="zen-review-method-btn" data-method="commit-push">
          <div class="zen-review-method-icon">⬆️</div>
          <div class="zen-review-method-info">
            <div class="zen-review-method-name">Commit & Push</div>
            <div class="zen-review-method-desc">Commit changes and push to current branch</div>
          </div>
        </button>
        <button class="zen-review-method-btn" data-method="pr">
          <div class="zen-review-method-icon">🔀</div>
          <div class="zen-review-method-info">
            <div class="zen-review-method-name">Create Pull Request</div>
            <div class="zen-review-method-desc">Push to new branch and open PR</div>
          </div>
        </button>
        <button class="zen-review-method-btn" data-method="comment">
          <div class="zen-review-method-icon">💬</div>
          <div class="zen-review-method-info">
            <div class="zen-review-method-name">Comment Only</div>
            <div class="zen-review-method-desc">Add comment to task/PR — no file changes</div>
          </div>
        </button>
        <button class="zen-review-method-btn zen-review-method-btn--custom" data-method="custom">
          <div class="zen-review-method-icon">✏️</div>
          <div class="zen-review-method-info">
            <div class="zen-review-method-name">Custom…</div>
            <div class="zen-review-method-desc">Describe your own update method</div>
          </div>
        </button>
      </div>
      <div class="zen-review-custom-input" style="display:none">
        <textarea id="zen-review-custom-method" placeholder="Describe the GitHub update method…" rows="3"></textarea>
      </div>
    </div>
    <div class="zen-review-actions">
      <button class="zen-review-btn zen-review-btn--alter" data-action="cancel-method">
        <span>←</span> Back
      </button>
      <button class="zen-review-btn zen-review-btn--accept" data-action="apply-method">
        <span>✓</span> Apply
      </button>
    </div>`;

  // Wire method selection
  let selectedMethod = null;
  card.querySelectorAll('.zen-review-method-btn').forEach(btn => {
    btn.addEventListener('click', function() {
      card.querySelectorAll('.zen-review-method-btn').forEach(b => b.classList.remove('zen-review-method-btn--selected'));
      this.classList.add('zen-review-method-btn--selected');
      selectedMethod = this.dataset.method;
      const customInput = card.querySelector('.zen-review-custom-input');
      if (customInput) customInput.style.display = selectedMethod === 'custom' ? '' : 'none';
    });
  });

  // Wire back/apply
  card.querySelector('[data-action="cancel-method"]').addEventListener('click', function() {
    // Restore original popup
    ZEN_REVIEW.plan = ZEN_REVIEW._savedPlan;
    const container = zenReviewGetContainer(opts.view);
    if (container) {
      const existing = container.querySelector('.zen-review-popup');
      if (existing) existing.remove();
      zenReviewShow(opts);
    }
  });

  card.querySelector('[data-action="apply-method"]').addEventListener('click', function() {
    if (!selectedMethod) return;
    const customInput = card.querySelector('#zen-review-custom-method');
    const customText = customInput ? customInput.value.trim() : '';

    // Update plan with new method
    if (ZEN_REVIEW.plan) {
      ZEN_REVIEW.plan.method = selectedMethod;
      if (selectedMethod === 'custom' && customText) {
        ZEN_REVIEW.plan.steps = [{ icon: '✏️', text: customText }];
        ZEN_REVIEW.plan.summary = 'Custom: ' + customText;
      }
    }

    // Restore original popup with updated plan
    const container = zenReviewGetContainer(opts.view);
    if (container) {
      const existing = container.querySelector('.zen-review-popup');
      if (existing) existing.remove();
      opts.plan = ZEN_REVIEW.plan;
      zenReviewShow(opts);
    }
  });
}

// ── Review Type Selector ────────────────────────────────────────
function zenReviewOpenReviewSelector(opts) {
  const popup = document.getElementById('zen-review-popup');
  if (!popup) return;

  const card = popup.querySelector('.zen-review-card');
  if (!card) return;

  card.innerHTML = `
    <div class="zen-review-header">
      <div class="zen-review-status-icon">👁️</div>
      <div class="zen-review-header-text">
        <div class="zen-review-title">Review Type</div>
        <div class="zen-review-subtitle">Choose what to review</div>
      </div>
    </div>
    <div class="zen-review-body">
      <div class="zen-review-method-list">
        <button class="zen-review-method-btn" data-review-type="backend">
          <div class="zen-review-method-icon">⚙️</div>
          <div class="zen-review-method-info">
            <div class="zen-review-method-name">Backend Review</div>
            <div class="zen-review-method-desc">Diff, files changed, validation results</div>
          </div>
        </button>
        <button class="zen-review-method-btn" data-review-type="frontend">
          <div class="zen-review-method-icon">🖥️</div>
          <div class="zen-review-method-info">
            <div class="zen-review-method-name">Frontend Review</div>
            <div class="zen-review-method-desc">Side-by-side Previous / After page comparison</div>
          </div>
        </button>
        <button class="zen-review-method-btn" data-review-type="fullstack">
          <div class="zen-review-method-icon">🔭</div>
          <div class="zen-review-method-info">
            <div class="zen-review-method-name">Full-Stack Review</div>
            <div class="zen-review-method-desc">Frontend comparison + backend diff, files, validation</div>
          </div>
        </button>
      </div>
    </div>
    <div class="zen-review-actions">
      <button class="zen-review-btn zen-review-btn--alter" data-action="cancel-review">
        <span>←</span> Back
      </button>
    </div>`;

  card.querySelectorAll('[data-review-type]').forEach(btn => {
    btn.addEventListener('click', function() {
      const type = this.dataset.reviewType;
      zenReviewDismiss();
      zenReviewOpen(type, opts);
    });
  });

  card.querySelector('[data-action="cancel-review"]').addEventListener('click', function() {
    const container = zenReviewGetContainer(opts.view);
    if (container) {
      const existing = container.querySelector('.zen-review-popup');
      if (existing) existing.remove();
      zenReviewShow(opts);
    }
  });
}

// ── Open Review Panel ──────────────────────────────────────────
function zenReviewOpen(type, opts) {
  ZEN_REVIEW.reviewType = type;
  const view = opts.view;

  // For full page chat, open review as an overlay
  // For drawer/small, expand the chat area
  const container = zenReviewGetContainer(view);
  if (!container) return;

  const existing = container.querySelector('.zen-review-popup');
  if (existing) existing.remove();

  const popup = document.createElement('div');
  popup.className = 'zen-review-popup zen-review-popup--review';
  popup.id = 'zen-review-popup';

  if (type === 'backend') {
    popup.innerHTML = zenReviewRenderBackend(opts);
  } else if (type === 'frontend') {
    popup.innerHTML = zenReviewRenderFrontend(opts);
  } else {
    popup.innerHTML = zenReviewRenderFullstack(opts);
  }

  container.appendChild(popup);
  zenReviewWireReviewListeners(popup, type, opts);
}

// ── Backend Review ─────────────────────────────────────────────
function zenReviewRenderBackend(opts) {
  const evidence = opts.evidence || [];
  const plan = opts.plan || {};
  const filesTouched = plan.filesTouched || evidence.filter(e => e.endsWith('.py') || e.endsWith('.js') || e.endsWith('.css') || e.endsWith('.html') || e.endsWith('.json'));

  return `
    <div class="zen-review-backdrop"></div>
    <div class="zen-review-card zen-review-card--review">
      <div class="zen-review-header">
        <div class="zen-review-status-icon">⚙️</div>
        <div class="zen-review-header-text">
          <div class="zen-review-title">Backend Review</div>
          <div class="zen-review-subtitle">Diff · Files · Validation</div>
        </div>
        <button class="zen-review-close" onclick="zenReviewDismiss()">✕</button>
      </div>
      <div class="zen-review-body">
        <!-- Toggle row -->
        <div class="zen-review-toggles">
          <label class="zen-review-toggle">
            <input type="checkbox" checked data-toggle="summary"> Human Summary
          </label>
          <label class="zen-review-toggle">
            <input type="checkbox" checked data-toggle="diff"> Raw Diff
          </label>
          <label class="zen-review-toggle">
            <input type="checkbox" checked data-toggle="files"> Files Changed
          </label>
          <label class="zen-review-toggle">
            <input type="checkbox" checked data-toggle="validation"> Validation
          </label>
        </div>

        <!-- Human Summary -->
        <div class="zen-review-section" data-section="summary">
          <div class="zen-review-section-label">HUMAN SUMMARY</div>
          <div class="zen-review-summary-text">${zenReviewEscape((opts.response || '').substring(0, 500))}</div>
        </div>

        <!-- Files Changed -->
        <div class="zen-review-section" data-section="files">
          <div class="zen-review-section-label">FILES CHANGED</div>
          <div class="zen-review-files">
            ${filesTouched.length ? filesTouched.map(f => `
              <div class="zen-review-file">
                <span class="zen-review-file-icon">📄</span>
                <span class="zen-review-file-name">${zenReviewEscape(f)}</span>
                <span class="zen-review-file-status">modified</span>
              </div>`).join('') : '<div class="zen-review-empty">No files changed</div>'}
          </div>
        </div>

        <!-- Validation -->
        <div class="zen-review-section" data-section="validation">
          <div class="zen-review-section-label">VALIDATION</div>
          <div class="zen-review-validation">
            ${(plan.checksRun || []).length ? plan.checksRun.map((c, i) => {
              const passed = (plan.checksPassed || []).includes(c);
              const failed = (plan.checksFailed || []).includes(c);
              const status = passed ? 'passed' : (failed ? 'failed' : 'not-run');
              return `<div class="zen-review-check zen-review-check--${status}">
                <span class="zen-review-check-icon">${passed ? '✅' : (failed ? '❌' : '○')}</span>
                <span class="zen-review-check-name">${zenReviewEscape(c)}</span>
                <span class="zen-review-check-status">${status}</span>
              </div>`;
            }).join('') : '<div class="zen-review-empty">No validation checks run</div>'}
          </div>
        </div>

        <!-- Diff placeholder (populated by API) -->
        <div class="zen-review-section" data-section="diff">
          <div class="zen-review-section-label">RAW DIFF</div>
          <div class="zen-review-diff" id="zen-review-diff">
            <div class="zen-review-empty">Loading diff…</div>
          </div>
        </div>
      </div>
      <div class="zen-review-actions">
        <button class="zen-review-btn zen-review-btn--alter" data-action="close-review">← Back to Gate</button>
        <button class="zen-review-btn zen-review-btn--accept" data-action="accept-from-review">✓ Accept</button>
      </div>
    </div>`;
}

// ── Frontend Review ────────────────────────────────────────────
function zenReviewRenderFrontend(opts) {
  return `
    <div class="zen-review-backdrop"></div>
    <div class="zen-review-card zen-review-card--review zen-review-card--wide">
      <div class="zen-review-header">
        <div class="zen-review-status-icon">🖥️</div>
        <div class="zen-review-header-text">
          <div class="zen-review-title">Frontend Review</div>
          <div class="zen-review-subtitle">Previous / After comparison</div>
        </div>
        <div class="zen-review-compare-tabs">
          <button class="zen-review-compare-tab active" data-compare="previous">Previous</button>
          <button class="zen-review-compare-tab" data-compare="after">After</button>
        </div>
        <button class="zen-review-close" onclick="zenReviewDismiss()">✕</button>
      </div>
      <div class="zen-review-body zen-review-compare-body">
        <div class="zen-review-compare-pane" id="zen-review-compare-pane">
          <div class="zen-review-compare-label">PREVIEW</div>
          <div class="zen-review-compare-frame">
            <iframe id="zen-review-compare-iframe" src="agent-os.html" sandbox="allow-same-origin allow-scripts"></iframe>
          </div>
        </div>
      </div>
      <div class="zen-review-actions">
        <button class="zen-review-btn zen-review-btn--alter" data-action="close-review">← Back to Gate</button>
        <button class="zen-review-btn zen-review-btn--accept" data-action="accept-from-review">✓ Accept</button>
      </div>
    </div>`;
}

// ── Full-Stack Review ──────────────────────────────────────────
function zenReviewRenderFullstack(opts) {
  return `
    <div class="zen-review-backdrop"></div>
    <div class="zen-review-card zen-review-card--review zen-review-card--wide">
      <div class="zen-review-header">
        <div class="zen-review-status-icon">🔭</div>
        <div class="zen-review-header-text">
          <div class="zen-review-title">Full-Stack Review</div>
          <div class="zen-review-subtitle">Frontend + Backend</div>
        </div>
        <button class="zen-review-close" onclick="zenReviewDismiss()">✕</button>
      </div>
      <div class="zen-review-body">
        <div class="zen-review-grid-2">
          <!-- Left: Frontend comparison -->
          <div class="zen-review-section">
            <div class="zen-review-section-label">FRONTEND</div>
            <div class="zen-review-compare-tabs">
              <button class="zen-review-compare-tab active" data-compare="previous">Previous</button>
              <button class="zen-review-compare-tab" data-compare="after">After</button>
            </div>
            <div class="zen-review-compare-frame zen-review-compare-frame--small">
              <iframe id="zen-review-fs-iframe" src="agent-os.html" sandbox="allow-same-origin allow-scripts"></iframe>
            </div>
          </div>
          <!-- Right: Backend summary -->
          <div>
            <div class="zen-review-section">
              <div class="zen-review-section-label">BACKEND SUMMARY</div>
              <div class="zen-review-summary-text">${zenReviewEscape((opts.response || '').substring(0, 400))}</div>
            </div>
            <div class="zen-review-section">
              <div class="zen-review-section-label">FILES CHANGED</div>
              <div class="zen-review-files">
                ${(opts.evidence || []).filter(e => e.match(/\.(py|js|css|html|json|md|txt)$/)).map(f => `
                  <div class="zen-review-file">
                    <span class="zen-review-file-icon">📄</span>
                    <span class="zen-review-file-name">${zenReviewEscape(f)}</span>
                  </div>`).join('') || '<div class="zen-review-empty">No files</div>'}
              </div>
            </div>
            <div class="zen-review-section">
              <div class="zen-review-section-label">RISK NOTES</div>
              <div class="zen-review-risk-notes">
                <div class="zen-review-risk-item">⚠️ Review diff before pushing</div>
                <div class="zen-review-risk-item">⚠️ Verify no secrets in commits</div>
              </div>
            </div>
          </div>
        </div>
      </div>
      <div class="zen-review-actions">
        <button class="zen-review-btn zen-review-btn--alter" data-action="close-review">← Back to Gate</button>
        <button class="zen-review-btn zen-review-btn--accept" data-action="accept-from-review">✓ Accept</button>
      </div>
    </div>`;
}

// ── Wire review panel listeners ────────────────────────────────
function zenReviewWireReviewListeners(popup, type, opts) {
  // Toggle sections
  popup.querySelectorAll('[data-toggle]').forEach(toggle => {
    toggle.addEventListener('change', function() {
      const section = this.dataset.toggle;
      const el = popup.querySelector('[data-section="' + section + '"]');
      if (el) el.style.display = this.checked ? '' : 'none';
    });
  });

  // Compare tabs (frontend / fullstack)
  popup.querySelectorAll('[data-compare]').forEach(tab => {
    tab.addEventListener('click', function() {
      popup.querySelectorAll('[data-compare]').forEach(t => t.classList.remove('zen-review-compare-tab--active'));
      this.classList.add('zen-review-compare-tab--active');
      const mode = this.dataset.compare;
      const iframe = popup.querySelector('iframe');
      if (iframe) {
        // Toggle iframe src to show previous vs after
        iframe.src = 'agent-os.html?review=' + mode;
      }
    });
  });

  // Back to gate
  popup.querySelector('[data-action="close-review"]')?.addEventListener('click', function() {
    zenReviewDismiss();
    zenReviewShow(opts);
  });

  // Accept from review
  popup.querySelector('[data-action="accept-from-review"]')?.addEventListener('click', function() {
    zenReviewAccept(opts);
  });

  // Load diff for backend review
  if (type === 'backend' || type === 'fullstack') {
    zenReviewLoadDiff(opts);
  }
}

// ── Load diff from API ─────────────────────────────────────────
function zenReviewLoadDiff(opts) {
  const diffEl = document.getElementById('zen-review-diff');
  if (!diffEl) return;

  fetch('/api/diff', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ sessionId: opts.sessionId, evidence: opts.evidence || [] })
  }).then(function(resp) {
      if (!resp.ok) throw new Error('HTTP ' + resp.status);
      return resp.json();
    })
    .then(function(data) {
      if (data.diff) {
        diffEl.innerHTML = '<pre class="zen-review-diff-code">' + zenReviewEscape(data.diff) + '</pre>';
      } else {
        diffEl.innerHTML = '<div class="zen-review-empty">No diff available</div>';
      }
    }).catch(function() {
      diffEl.innerHTML = '<div class="zen-review-empty">Could not load diff</div>';
    });
}

// ── Dismiss popup ──────────────────────────────────────────────
function zenReviewDismiss() {
  ZEN_REVIEW.active = false;
  const popup = document.getElementById('zen-review-popup');
  if (popup) popup.remove();
}

function zenReviewKeepPopupVisible(popup, view) {
  const suffix = view === 'small' ? '-small' : (view === 'drawer' ? '-drawer' : '');
  const messages = document.getElementById('zen-chat' + suffix + '-messages');
  if (messages && popup && popup.parentElement === messages) {
    messages.appendChild(popup);
    messages.scrollTop = messages.scrollHeight;
  }
}

// ── Chat input block/unblock ───────────────────────────────────
function zenChatBlockInput(view) {
  const suffix = view === 'small' ? '-small' : (view === 'drawer' ? '-drawer' : '');
  const input = document.getElementById('zen-chat' + suffix + '-input');
  const btn = document.getElementById('zen-chat' + suffix + '-send');
  if (input) { input.disabled = true; input.classList.add('zen-chat-input--blocked'); }
  if (btn) { btn.disabled = true; btn.classList.add('zen-chat-send--blocked'); }
}

function zenChatUnblockInput(view) {
  const suffix = view === 'small' ? '-small' : (view === 'drawer' ? '-drawer' : '');
  const input = document.getElementById('zen-chat' + suffix + '-input');
  const btn = document.getElementById('zen-chat' + suffix + '-send');
  if (input) { input.disabled = false; input.classList.remove('zen-chat-input--blocked'); input.focus(); }
  if (btn) { btn.classList.remove('zen-chat-send--blocked'); btn.disabled = !(input && input.value.trim()); }
}

window.ZEN_REVIEW = ZEN_REVIEW;
window.ZenReviewGate = {
  show: zenReviewShow,
  dismiss: zenReviewDismiss,
  buildPlan: zenReviewBuildPlan,
  openReview: zenReviewOpen,
  accept: zenReviewAccept,
  alter: zenReviewAlter,
  executePlan: zenReviewExecutePlan
};

// ── Helpers ────────────────────────────────────────────────────
function zenReviewEscape(str) {
  return String(str || '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

function zenReviewRiskClass(opts) {
  const evidence = opts.evidence || [];
  if (evidence.some(e => e.includes('test') || e.includes('spec'))) return 'low';
  if (evidence.some(e => e.includes('.py') || e.includes('.js'))) return 'medium';
  return 'low';
}

function zenReviewRiskLabel(opts) {
  const cls = zenReviewRiskClass(opts);
  return cls === 'low' ? 'LOW RISK' : (cls === 'medium' ? 'MEDIUM RISK' : 'HIGH RISK');
}

// ── Integration: Override zenChatSend completion ───────────────
// Store original zenChatSend
const _zenChatSendOriginal = zenChatSend;

zenChatSend = function(view) {
  const suffix = view === 'small' ? '-small' : (view === 'drawer' ? '-drawer' : '');
  const input = document.getElementById('zen-chat' + suffix + '-input');
  if (!input) return;
  const text = input.value.trim();
  if (!text) return;
  input.value = '';
  input.style.height = 'auto';

  // Check if chat is blocked (review gate open)
  if (input.disabled) return;

  zenChatAddMessage('user', text, view);
  zenChatSetStatus('working', view);
  zenChatUpdateSendBtn(view);

  var sessionId = zenChatState.activeSession;
  var info = zenWorkSessionInfo(sessionId);
  var work = zenWorkStart('thinking', {
    sessionId: sessionId,
    prompt: text,
    title: info.title || info.label,
    action: text.toLowerCase().includes('plan') ? 'Reviewing session context and forming a plan.' : 'Reviewing session context.',
    modelRole: text.toLowerCase().includes('plan') ? 'Chat / Plan model' : 'Chat model',
    boundary: 'Planning only',
    activityLog: [
      { icon:'💬', label:'prompt', text:'Received prompt from Zen Chat.', ts: zenWorkNow(), status:'done' },
      { icon:'🧠', label:'context', text:'Reviewing session context.', ts: zenWorkNow(), status:'active' }
    ]
  });
  zenWorkAddActivity(work.id, '📌', 'boundary', 'Planning only — no file writes or GitHub update.', 'done');
  var runningTimer = setTimeout(function(){
    zenWorkTransition(work.id, 'running', {
      action: 'Executing accepted session request.',
      modelRole: 'Execution model',
      boundary: 'Execution boundary · local only · no GitHub update'
    });
    zenWorkAddActivity(work.id, '🌉', 'bridge', 'Forwarding request through Agent OS → Hermes bridge.', 'active');
  }, 900);

  zenWorkAddActivity(work.id, '📡', 'api', 'POST /api/sessions/' + sessionId + '/messages', 'active');

  zenChatApiStreamMessage(sessionId, { role: 'user', content: text }, work.id).then(function(resp) {
    clearTimeout(runningTimer);
    var hermesContent = resp && resp.assistant && resp.assistant.content ? resp.assistant.content : '';
    var hermesProgress = resp && Array.isArray(resp.progress) ? resp.progress.join('\n') : '';
    if (hermesProgress || hermesContent) zenWorkAddHermesActions(work.id, [hermesProgress, hermesContent].filter(Boolean).join('\n'));
    zenWorkAddActivity(work.id, '🦉', 'hermes', resp && resp.assistant ? 'Hermes response received.' : 'Hermes bridge completed; refreshing session history.', 'done');
    zenWorkAddActivity(work.id, '🔄', 'refresh', 'Refreshing visible chat history from Hermes state.', 'active');
    zenWorkTransition(work.id, 'complete', {
      action: 'Task complete / Ready for review.',
      modelRole: 'Execution model',
      boundary: 'Review gate · GitHub update requires approval'
    });

    // Build evidence from Hermes response
    const evidence = zenReviewExtractEvidence(hermesContent);

    // Build GitHub update plan
    const plan = zenReviewBuildPlan({
      filesTouched: evidence.filter(e => e.match(/\.(py|js|css|html|json|md|txt)$/)),
      hasChanges: evidence.length > 0,
      checksRun: evidence.filter(e => e.includes('check') || e.includes('test') || e.includes('lint')),
      checksPassed: [],
      checksFailed: [],
      updateTask: true,
    });

    // Refresh chat then show review gate
    zenChatRefreshActiveMessages(true).then(function() {
      zenChatSetStatus('idle', view);
      zenChatUpdateSendBtn(view);
      zenWorkRenderAll();

      // Show completion popup
      zenReviewShow({
        workId: work.id,
        view: view,
        sessionId: sessionId,
        prompt: text,
        response: hermesContent,
        progress: resp && resp.progress ? resp.progress : [],
        plan: plan,
        evidence: evidence,
        status: 'complete'
      });
    });
  }).catch(function(err) {
    clearTimeout(runningTimer);
    var msg = err && err.message ? err.message : 'Server error';
    var cause = 'Hermes bridge request failed.';
    var recovery = ['Retry same prompt', 'Alter plan', 'Open logs'];
    if (/timeout/i.test(msg)) { cause = 'Execution timeout or blocked process.'; recovery = ['Retry with simpler request', 'Check server logs', 'Increase timeout']; }
    if (/token|context/i.test(msg)) { cause = 'Token/context limit hit.'; recovery = ['Shorten request', 'Split into smaller tasks', 'Switch model']; }
    if (/network|fetch|reach/i.test(msg)) { cause = 'Agent OS could not reach Hermes.'; recovery = ['Check Hermes process', 'Retry', 'Check network']; }
    if (/permission|denied|auth/i.test(msg)) { cause = 'Permission or authentication failure.'; recovery = ['Check credentials', 'Verify permissions', 'Re-authenticate']; }
    if (/not found|404/i.test(msg)) { cause = 'File or resource not found.'; recovery = ['Check file paths', 'Verify resource exists', 'Alter request']; }
    if (/quota|rate.limit|429/i.test(msg)) { cause = 'Model quota or rate limit exceeded.'; recovery = ['Wait and retry', 'Switch model', 'Check quota']; }

    zenWorkAddActivity(work.id, '⚠️', 'failure', cause, 'failed');
    zenWorkTransition(work.id, 'failed', {
      action: 'Diagnosis required before retry.',
      modelRole: 'Execution model',
      boundary: 'Recovery gate',
      cause: cause,
      evidence: msg,
      recovery: recovery
    });
    zenWorkRenderAll();

    // Show failure diagnosis popup
    zenReviewShow({
      workId: work.id,
      view: view,
      sessionId: sessionId,
      prompt: text,
      response: '',
      progress: [],
      plan: null,
      evidence: [],
      status: 'failed',
      diagnosis: {
        cause: cause,
        evidence: msg,
        recovery: recovery
      }
    });
  });
};

// ── Extract evidence from Hermes response ──────────────────────
function zenReviewExtractEvidence(content) {
  if (!content) return [];
  const evidence = [];
  // Look for file paths mentioned in the response
  const filePattern = /[\w\-./]+\.(py|js|css|html|json|md|txt|yaml|yml|sh|bash)/g;
  const matches = content.match(filePattern);
  if (matches) {
    matches.forEach(m => { if (!evidence.includes(m)) evidence.push(m); });
  }
  // Look for check/test mentions
  const checkPattern = /(?:check|test|lint|validate|verify|run)\s+[\w\-./]+/gi;
  const checkMatches = content.match(checkPattern);
  if (checkMatches) {
    checkMatches.forEach(m => { if (!evidence.includes(m)) evidence.push(m); });
  }
  return evidence;
}
