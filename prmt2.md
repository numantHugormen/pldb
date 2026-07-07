New and improved prompt:

You are an expert Principal Cloud Data Architect and a seasoned Agile Project Manager specializing in Azure, Databricks, and modern DataOps. 

Your task is to deliver a comprehensive, production-ready Technical Project Plan for implementing Azure Databricks across a multi-division organization. The final output must be structured as a high-quality GitHub Markdown (.md) file, utilizing Mermaid.js for architecture diagrams and clear placeholders for image assets or external documentation links.

---

### 1. Current State & Background Context
*   **Ingestion:** Qlik Replicate lands data into Azure Data Lake Storage (ADLS) and an Azure SQL Managed Instance (SQL MI).
*   **Ingestion:** Data from a GE Historian is downloaded to ADLS storage (ADLS)
*   **Ingestion:** Data from MES system is transferred to SQL MI
*   **Orchestration & Compute:** Azure Data Factory (ADF) orchestrates data movement into an Enterprise Data Warehouse (EDW) hosted on SQL MI.
*   **Transformation:** Heavy reliance on SQL MI Stored Procedures for T-SQL transformations. Can they easily be converted into lake flows and dlt’s
*   **CI/CD:** Moving toward a mature DevOps model using Azure DevOps (ADO) pipelines and an Agile approach to development

---

### 2. Architectural & Technical Requirements
Your project plan must provide detailed implementation steps and the underlying technical "why" for each of the following areas:

*   **Workspace & Data Governance (Hub-and-Spoke):** Design an easy-to-onboard architecture for multiple divisions. Explicitly address the trade-offs between deploying separate Databricks workspaces per division versus leveraging Unity Catalog (isolated Catalogs/Schemas) within a single/fewer workspaces.
*   **Diagram an effective hub and spoke model, that will allow onboarding of multiple data segmented divisions and departments
*   **Answer the question whether you should have a workspace for dev, eat and prod or if you would just use catalogs and schemas *   **Explain the purpose of the bronze, silver and gold layer, and give examples of which computes are used and how data is typically partitioned
*   **Explain the ETL and ELT tools to load bronze, silver and gold layer
*   **Explain the tooling needed to build ML models, endpoint and creating agents
*   **Security, Identity, & Network Compliance:** Detail the implementation of a zero-trust architecture. Provide the technical steps for:
    *   Configuring Azure Active Directory (Entra ID) / SAML authentication and mapping Unity Catalog RBAC roles.
    *   Securing data at rest using Bring Your Own Key (BYOK) encryption integrated with Azure Key Vault.
    *   Securing data in transit and isolating the compute plane via VNet injection and Azure Private Link.
*   **ETL Migration Path:** Detail how to migrate the legacy SQL MI stored procedures into Databricks. Specify the exact compute types (e.g., Photon-accelerated, Jobs Compute, All-Purpose) required for different types of transformation tasks.
*   **Cost Optimization & Pipeline Automation:** Provide actionable strategies for keeping compute costs low. Address whether starting/stopping compute clusters directly within ADO Pipeline runs is an industry best practice, and outline alternative auto-scaling or serverless strategies.
*   **BI & Reporting Layer:** Define the optimal architecture to support Power BI reporting. Address whether to keep a Serverless SQL Warehouse running continuously, or how to manage compute efficiently without sacrificing dashboard performance.
*   **MLOps & Generative AI:** Lay out the compute management, deployment strategies, and architecture for agent development, ML model deployment, and real-time/batch inference endpoints.

---

### 3. Agile Project Structure (ADO Integration)
To allow for seamless ingestion into Azure DevOps, map out the entire project lifecycle utilizing the standard Agile hierarchy. Provide explicit, technical descriptions for:
1.  **Epics:** High-level project phases (e.g., Security & Governance, Core Ingestion, MLOps Track).
2.  **Features:** Parent deliverables within those Epics.
3.  **User Stories:** Acceptance-criteria-driven requirements written from the developer/architect perspective.
4.  **Tasks:** Granular technical action items for each story.

---

### 4. Deliverable Format Constraints
*   **File Type:** Valid GitHub-Flavored Markdown (.md).
*   **Visuals:** Use Mermaid.js code blocks for any architecture diagrams (e.g., workspace hierarchy, CI/CD branching strategy, data flow).
*   **References:** Use clean Markdown syntax for links and asset placeholders (e.g., `![Architecture Diagram](./images/architecture.png)`).
*   **Tone:** Deeply technical, instructional, and structured for an engineering team.
