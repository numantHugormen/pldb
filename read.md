Here’s a full build for a scheduled, production-grade export from a SharePoint list into ADLS using Azure Data Factory. It covers authentication, linked services, a parameterized pipeline, incremental loads, the trigger, and promotion across environments.

Architecture at a glance
SharePoint Online List ──(OData, cert-based service principal)──► ADF Copy activity ──► ADLS Gen2
                                                                     │                   raw/sharepoint/<list>/load_date=YYYY-MM-DD/*.parquet
                                         Key Vault (cert) ◄──────────┘
                                         Schedule trigger, alerts
Step 1 — Create the authentication certificate

The ADF SharePoint Online List connector authenticates with a service principal. Use certificate authentication. The client-secret option depends on Azure ACS, which Microsoft is retiring, and the connector docs tell you to switch to certificates.

Generate a certificate in Key Vault: Key Vault → Certificates → Generate/Import. Use a self-signed certificate (or one from your company’s certificate authority), with content type PKCS #12. The ADF connector requires that format.
Download the public .cer file. You’ll upload it to the app registration in Step 2.
Note the certificate’s secret name. Key Vault exposes the PFX as a secret, base64 encoded, under the same name as the certificate. ADF will reference it in Step 4.
Step 2 — Register the Entra app
Entra ID → App registrations → New registration. Use a name like adf-sharepoint-reader-<env>, single tenant, no redirect URI.
Write down the Application (client) ID and the Tenant ID.
Certificates & secrets → Certificates → Upload the .cer file from Step 1.
API permissions → Add a permission → SharePoint. Choose SharePoint, not Microsoft Graph, because the connector talks to SharePoint’s OData endpoint. Then choose Application permissions.
Simplest: Sites.Read.All. This is what Microsoft’s documentation shows.
Least privilege (recommended): Sites.Selected. A SharePoint admin then has to grant the app read on just the target site, for example with PnP PowerShell Grant-PnPAzureADAppSitePermission -AppId <id> -DisplayName <name> -Site <siteUrl> -Permissions Read. Your security team will usually prefer this. Validate it in dev first.
Grant admin consent.
Step 3 — Grant ADF access to Key Vault and ADLS

ADF’s system-assigned managed identity needs two role assignments:

Resource	Role
Key Vault	Key Vault Secrets User (if the vault uses RBAC), or a Get/List secrets access policy
ADLS Gen2 storage account or container	Storage Blob Data Contributor

If the storage account or Key Vault has public access disabled, use a Managed VNet integration runtime with managed private endpoints, or a self-hosted integration runtime. SharePoint Online itself is reached over the public internet.

Step 4 — Create the linked services

a) Key Vault: LS_KeyVault, authenticated with ADF’s managed identity.

b) SharePoint Online List: LS_SharePointList. Parameterize the site URL so one linked service can serve several sites:

json
{
  "name": "LS_SharePointList",
  "properties": {
    "type": "SharePointOnlineList",
    "parameters": { "siteUrl": { "type": "String" } },
    "typeProperties": {
      "siteUrl": "@{linkedService().siteUrl}",
      "tenantId": "<tenant-id>",
      "servicePrincipalId": "<app-client-id>",
      "servicePrincipalCredentialType": "ServicePrincipalCert",
      "servicePrincipalEmbeddedCert": {
        "type": "AzureKeyVaultSecret",
        "store": { "referenceName": "LS_KeyVault", "type": "LinkedServiceReference" },
        "secretName": "adf-sharepoint-cert"
      }
    }
  }
}

Click Test connection with a real site URL. If the test fails, the cause is almost always one of three things: admin consent wasn’t granted, the Sites.Selected grant is missing, or the certificate is in the wrong format.

c) ADLS Gen2: LS_ADLS_Raw, authenticated with the managed identity.

Step 5 — Create parameterized datasets

Source: DS_SP_List

json
{
  "name": "DS_SP_List",
  "properties": {
    "type": "SharePointOnlineListResource",
    "linkedServiceName": {
      "referenceName": "LS_SharePointList", "type": "LinkedServiceReference",
      "parameters": { "siteUrl": "@dataset().siteUrl" }
    },
    "parameters": { "siteUrl": {"type":"String"}, "listName": {"type":"String"} },
    "typeProperties": { "listName": "@dataset().listName" }
  }
}

The dataset takes the list’s display name, and the name can’t contain an apostrophe.

Sink: DS_ADLS_Parquet. Parameterize container, folderPath, and fileName, and set the compression to snappy.

Parquet keeps column data types. If the people consuming the export want CSV, add a second sink dataset for that.

Step 6 — Build the pipeline (full load first)

Create the pipeline PL_SP_List_Export with these parameters:

Parameter	Example
siteUrl	https://contoso.sharepoint.com/sites/Ops
listName	Asset Register
targetFolder	sharepoint/asset_register

Add a Copy data activity:

Source: DS_SP_List, passing in the parameters.
Query (optional): an OData filter, for example $select=Id,Title,Status,Modified,Created. Selecting columns explicitly keeps the schema stable if someone adds columns to the list.
Request timeout: raise it from the 300-second default for large lists.
Sink: DS_ADLS_Parquet, with
Folder path: @concat(pipeline().parameters.targetFolder, '/load_date=', formatDateTime(utcNow(),'yyyy-MM-dd'))
File name: @concat(replace(pipeline().parameters.listName,' ','_'), '_', formatDateTime(utcNow(),'yyyyMMddHHmmss'), '.parquet')
Mapping: click Import schemas. Rename the SharePoint internal column names (for example field_3 or Asset_x0020_Tag) to clean names here.
Additional columns: add _extract_ts = @utcNow() and _source_list = @pipeline().parameters.listName for lineage.

Debug-run the pipeline once and check the Parquet output in ADLS.

Step 7 — Optional: incremental loads with a watermark

For large lists, avoid a full extract on every run:

Store a watermark. Keep the last successful Modified timestamp in a control table (Azure SQL or SQL MI) or in a small JSON file in ADLS.
Lookup activity reads the watermark.
Copy activity sets its source query to:
   $filter=Modified gt datetime'@{activity('LookupWatermark').output.firstRow.watermark}'

The datetime literal syntax depends on the OData version the endpoint uses, so test the filter in debug before relying on it.
4. Update the watermark after a successful copy, with a Stored Procedure activity or a Script activity, setting it to the max Modified value from that run’s start.

An incremental Modified filter doesn’t capture deleted items. If deletes matter downstream, schedule a periodic full snapshot (say, weekly) or a separate ID-only extract to reconcile against.

Step 8 — Add the schedule trigger

Add trigger → New/Edit → Schedule:

Recurrence: for example daily at 06:00.
Time zone: Eastern Standard Time. ADF handles daylight saving time when you pick a named time zone.
Parameters: supply the site, list, and folder values.
Publish the trigger, and confirm it shows as Started under Manage → Triggers.

To export several lists, put a ForEach over a JSON array, or over a control table read with a Lookup, inside one pipeline. That’s cleaner than one trigger per list.

Step 9 — Monitoring and alerting
Configure diagnostic settings on the factory to send to Log Analytics, with PipelineRuns and ActivityRuns enabled.
Create an Azure Monitor alert on the Failed pipeline runs metric, filtered to this pipeline, routed to an action group (email or Teams).
Add retry on the Copy activity (for example 2 retries, 120 seconds apart) to absorb SharePoint throttling.
Step 10 — Promote across dev, uat, and prod
Git-integrate the dev factory, then deploy through ARM templates or the @microsoft/azure-data-factory-utilities npm package in Azure DevOps or GitHub Actions.
Override siteUrl, servicePrincipalId, the Key Vault URL, and the storage account per environment with global parameters or a custom ARM parameter definition.
Use one app registration per environment, so dev credentials can never read prod sites.
Connector limitations to plan around
Issue	Impact / workaround
Person/Group and Lookup columns come through as Int32 IDs	Also export the lookup target list (and the site’s User Information List for people), then join downstream.
Managed Metadata, Attachments, External Data not supported	Use a Web activity or Python with Microsoft Graph for those columns.
Multi-select Choice	You can’t select more than one choice-type column via the connector. Test this against your list’s schema early.
Large lists / throttling	Use the incremental pattern, column $select, retries, and a longer timeout.
Display name vs internal name	Clean up the names in the Copy activity’s mapping tab.

If the list relies heavily on Person, Managed Metadata, or multi-choice columns, the Python/Graph approach from my earlier answer will be less painful than fighting the connector.

Sources:
