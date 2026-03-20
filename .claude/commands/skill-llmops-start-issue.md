Start working on a GitHub issue. Usage: /skill-llmops-start-issue <issue-number>

## Steps

### 1. Fetch issue details

Use the `mcp__github__get_issue` tool to fetch the issue from the repo:
- owner: `llmops-databricks-1`
- repo: `llmops-databricks-course-victor-kuznetsov`
- issue_number: the number passed as argument

### 2. Determine branch type

Look at the issue title and labels:
- If it describes a bug, defect, or fix → type is `bug`
- Otherwise → type is `feature`

### 3. Create a branch

Branch naming convention: `<type>/<issue-number>-<kebab-case-issue-title>`

Example: issue #5 titled "Add Delta ingestion pipeline" → `feature/5-add-delta-ingestion-pipeline`

Run:
```bash
git checkout main && git pull origin main
git checkout -b <branch-name>
```

If already on a non-main branch, warn the user and ask before switching.

### 4. Understand the issue

Read the issue title, description, and any acceptance criteria carefully.

Think through:
- What needs to be built or changed?
- What are the deliverables?
- Are there any ambiguities or missing details?

### 5. Ask clarifying questions if needed

If anything is unclear — scope, acceptance criteria, technical approach, dependencies — list your questions clearly and **wait for the user's answers before proceeding**.

If the issue is fully clear, summarize your understanding in 3–5 bullet points and propose next steps.
