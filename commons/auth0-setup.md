# **Auth0 Setup**

This document outlines the steps required to set up and configure **Auth0.**

---

# **Table of Contents**

1. **Create API**: Steps to create and configure an API in Auth0.
2. **Create an Application**: How to set up applications that will interact with the API.
3. **Create an Organization**: Creating and managing organizations within Auth0.
4. **User Management**: Managing roles, permissions, and users within Auth0.
5. **Authentication Configuration**: Setting up authentication methods and testing them.
6. **Add Triggers**: How to add triggers in Auth0.
7. **Personalised Steps**: Customised setup instructions specific to our project.
8. **Troubleshooting**: Some common issues and how to resolve them.

---

# **1. Create API**

### **Step 1: Create the API**

- Navigate to **Applications → APIs** in the sidebar.
- Click **Create API**.
- **Name:** `Example API`
- **Identifier:** `https://example-api.com` (This value will be used as the audience parameter on authorization calls.).
- **Jwt Profile:** Auth0
- **Signing Algorithm:** **RS256**
- Click **Create**.

### **Step 2: Enable RBAC and Permissions**

- After creating the API, go to the **Settings** tab of **Example API**.
- Scroll down to the **RBAC Settings** section.
- Enable **RBAC** (Role-Based Access Control).
- Enable the **Add Permissions in the Access Token** flag to include permissions in the access token.
- Click **Save Changes**.

### **Step 3: Add Permissions to the API**

- Go to the **Permissions** section.
- Click **Add Permission**.
- Add the relevant permissions for API, such as:
    - `admin:read`
    - `admin:write`
- Click **Save**.

---

# **2. Create an Application**

### **Step 1: Create the Application**

- Navigate to **Applications → Applications**
- Click **Create Application**.
- **Name:** `Example Web Application`
- **Application Type:** **Machine to Machine (In case of Backend Application)**
- Click **Create**.

### **Step 2: Configure the API in our Application**

- Once our application is created, go to **Applications → Example Web Application**.
- Under the **APIs** section, link your **Example API** by clicking **Add API**.
- Select the **Example API** and choose the required **Scopes** (e.g., `read:data`, `write:data`).
- Click **Save**.

### **Step 3: Enable CORS for Your Application**

- Go to **Applications → Example Web Application**.
- Under the **Settings** section, scroll down to find **Cross-Origin Authentication.**
- **Enable** **Allow Cross-Origin Authentication**.
- Click **Save Changes**.

### **Step 4: Test Using cURL to Get the Token**

- To test the OAuth2 setup, use the following cURL command to request an access token:

    ```bash
    curl --request POST \
      --url https://YOUR_AUTH0_DOMAIN/oauth/token \
      --header 'content-type: application/json' \
      --data '{
        "client_id": "YOUR_CLIENT_ID",
        "client_secret": "YOUR_CLIENT_SECRET",
        "audience": "https://example-api.com",
        "grant_type": "client_credentials"
      }'

    ```

- This will return an access token that you can use to authenticate requests to the **Example API**.

---

# **3. Create an Organization**

### **Step 1: Create an Organization**

- Navigate to **Organizations.**
- Click **Create Organization**.
- **Name:** `Example Organization`
- Click **Add Organization**.

### **Step 2: Enable Required Connections for the Organization**

- Under the **Connections** tab for the new organization, enable the required authentication methods:
    - **Google Sign-In:** If we want users to log in with Google, enable the **Google** connection.
    - **User Password Authentication:** If we want password-based login, enable the **User Password Authentication** connection. ( Enable allow signups if we want users to signup)

---

# **4. User Management**

### **Step 1: Create Roles**

- Navigate to **User Management → Roles**.
- Click **Create Role**.
- **Role Name:** `Default-Role`.
- Add the relevant **permissions.**
- Save your roles.

### **Step 2: Create Users**

- Navigate to **User Management → Users**.
- Click **Create User**.
- **Email:** Enter the user’s information.
- **Password:** Set a password.
- Click **Create**.

(if **User Password Authentication** is enabled for the organization, we can use need to use this user credentials to login)

---

# **5. Authentication Configuration**

### **Step 1: Test Database Connections**

- Navigate to **Authentication → Database**.
- Select the Database, test the connection by clicking **Try Connection**.

### **Step 2: Test Socials Connections**

- Navigate to **Authentication → Socials**
- Test **Social Connections** based on the connections set for organization.

### **Step 3: Test Authentication Profile**

- Navigate to **Authentication → AuthenticationProfile**
- Choose the Login flow and test by clicking on **Try**
- Verify that the **Authentication Profile** is working as expected.

---

# 6. **Add Triggers**

- Navigate to **Auth0 Dashboard → Actions → Library**.
- Click **Create Action** to create a new trigger.
- Choose the relevant trigger type:
    - **Pre User Registration**: Triggered before a user is registered in the system.
    - **Post Login**: Triggered after a user has logged in successfully.
- Define the action logic (JavaScript code) and set up any necessary conditions.
- Choose the flow for the action (e.g., pre-registration or post-login).
- Save the action and deploy it.

# 7. Personalised Steps

The following steps are personalised for our app setup. Please refer to the **generic steps above** for the base setup and follow these personalised instructions for specific configuration:

## **1. Create API**

### **Step 1: Create the API**

1. Navigate to **Applications → APIs** in the sidebar.
2. Click **Create API**.
3. **Name:** `Fulcrum API`
4. **Identifier:** `https://fulcrum-api-identifier`
5. **Jwt Profile:** Auth0
6. **Signing Algorithm:** **RS256**
7. Click **Create**.

### **Step 2: Enable RBAC and Permissions**

1. Go to **Settings** in the **Fulcrum API**.
2. Enable **RBAC** and check the box for **Add Permissions in the Access Token**.
3. Save changes.

### **Step 3: Add Permissions to the API**

1. Navigate to the **Permissions** tab.
2. Click **Add Permission** and add the necessary permissions, such as:
    - `admin:read` - Read all tenants
    - `admin:write` -Write permission to update tenants settings, external integrations, manage query metadata
    - `user:read` -Read Permission for user related activities
    - `user:write` -Write Permission for user related activities
    - `analysis:*` - Permission to allow all the operations on analysis manager service
    - `query:*` - Permission to allow all the operations on query manager service
    - `story:*` - Permission to allow all the operations on story manager service
    - `alert_report:read` - Read Permission for Alerts and Reports related activities
    - `alert_report:write` - Write Permission for Alerts and Reports related activities
3. Save changes.

---

## **2. Create Applications**

### **Default App Settings**

This app is created when we create a new tenant. We need to make a few updates:

1. Navigate to **Applications → Applications**.
2. Select **Default App**
3. Enable **CORS** and add the following Allowed Origins:
    - `https://dev-app.leverslabs.com`
    - `https://api.leverslabs.com`
    - `https://app.leverslabs.com`
4. Under the **APIs** tab, link the **Auth0 Management API** and select the scopes,
    - `update:users`
    - `update:users_app_metadata`
5. Under the **Connections** tab, enable:
    - **Database (Username-Password-Authentication)**
    - **Social (google-oauth2)**

### **Backend (Machine-to-Machine Apps)**

- **Analysis Manager**
    1. Navigate to **Applications → Applications**.
    2. Click **Create Application**.
    3. **Name:** `Analysis Manager`
    4. **Application Type:** `Machine to Machine`
    5. Enable **CORS** and add the following Allowed Origins:
        - `https://dev-app.leverslabs.com`
        - `https://api.leverslabs.com`
        - `https://app.leverslabs.com`
    6. Under the **APIs** tab, link the **Fulcrum API** and select the necessary scopes (`analysis:*`, `query:*`, `admin:read`).
    7. Save the application.
- **Query Manager**
    1. Create a new **Machine to Machine** application called `Query Manager`.
    2. Enable **CORS** and add the same Allowed Origins as the previous app.
    3. Link the **Fulcrum API** and select the required scopes (`admin:read`, `query:*`).
    4. Save the application.
- **Insights Backend**
    1. Create a new **Machine to Machine** application called `Insights Backend`.
    2. Enable **CORS** and add the same Allowed Origins.
    3. Link the **Fulcrum API** and select the required scopes (`admin:read`, `admin:write`, `user:read`, `user:write`, `alert_report:read`, `alert_report:write`).
    4. Save the application.
- **Story Manager**
    1. Create a new **Machine to Machine** application called `Story Manager`.
    2. Enable **CORS** and add the same Allowed Origins.
    3. Link the **Fulcrum API** and select the required scopes (`query:*`, `story:*`).
    4. Save the application.
- **Task Manager**
    1. Create a new **Machine to Machine** application called `Task Manager`.
    2. Enable **CORS** and add the same Allowed Origins.
    3. Link the **Fulcrum API** and select **all scopes**.
    4. Save the application.

---

### **Frontend (Regular Web Apps)**

- **Insights Web - Dev**
    1. Create a **Regular Web Application** called `Insights Web - Dev`.
    2. Update the **Application URLs** as follows:
        - **Application Login URL**: `https://app-dev.leverslabs.com/login`
        - **Allowed Callback URLs**: `https://*.vercel.app/api/auth/callback`, `https://insights-web-app-dev.vercel.app/api/auth/callback`
        - **Allowed Logout URL**: `https://*.vercel.app/login`
        - **Allowed Web Origins**: `https://app-dev.leverslabs.com/`, `https://*.vercel.app`, `https://insights-web-app-dev.vercel.app/`
    3. Enable **CORS**.
    4. Link the **Fulcrum API** and select **all scopes**.
    5. In the **Organizations** tab, set:
        - **Types of Users**: `Business Users`
        - **Login Flow**: `Prompt for Organization`
    6. Save the application.
- **Insights Web - Prod**
    1. Create a **Regular Web Application** called `Insights Web - Prod`.
    2. Update the **Application URLs**:
        - **Application Login URL**: `https://app.leverslabs.com/login`
        - **Allowed Callback URL**: `https://app.leverslabs.com/api/auth/callback`
        - **Allowed Logout URL**: `https://app.leverslabs.com/login`
        - **Allowed Web Origins**: `https://app.leverslabs.com/`
    3. Enable **CORS**.
    4. Link the **Fulcrum API** and select **all scopes**.
    5. In the **Organizations** tab, set:
        - **Types of Users**: `Business Users`
        - **Login Flow**: `Prompt for Organization`
    6. Save the application.

## **3. Create Organization**

### **Step 1: Create an Organization**

1. Navigate to **Organizations**.
2. Click **Create Organization**.
3. **Name:** `Levers Demo`
4. **Display Name:** `Leverslab Demo Organization`
5. Add **tenant_id** in Metadata to sync with the database.
6. Save the organization.

### **Step 2: Enable Required Connections**

1. In the **Connections** tab, enable the following:
    - **Username-Password-Authentication**
    - **Google-oauth2** (for Google sign-ins)
2. Save changes.

## **4. User Management**

### **Step 1: Create Default Role**

1. Navigate to **User Management → Roles**.
2. Click **Create Role**.
3. **Role Name**: `Fulcrum-Default`
4. Add all needed **permissions,**
    - `admin:read`
    - `admin:write`
    - `user:read`
    - `user:write`
    - `analysis:*`
    - `query:*`
    - `story:*`
    - `alert_report:read`
    - `alert_report:write`
5. Save the role.

# 5. Add Triggers

## **Pre User Registration Trigger**

1. Navigate to  **Actions → Triggers**.
2. Choose **pre-user-registration**.
3. Choose **Add Action → pre-register-action**

```json
const supportedDomains = ["vercel", "levers", "replit", "velotio", "enverus", "setup", "fulcrum"];

exports.onExecutePreUserRegistration = async (event, api) => {
    const userEmail = (event.user.email ?? "").toLowerCase();

    const allowRegistration = supportedDomains.some(domain => userEmail.includes(domain));

    if (!allowRegistration) {
        const LOG_MESSAGE = "Registration denied"
        const USER_MESSAGE = "Registration denied - You cannot signup using this email address"
        api.access.deny(LOG_MESSAGE, USER_MESSAGE)
    }
};

```

This **Pre User Registration Trigger** adds/registers the user in Auth0. The logic here prevents users from registering if their email domain isn't in the approved list (`vercel`, `levers`, `replit`, etc.). If the domain doesn't match, the registration is blocked and the user receives an error message indicating that their email domain is not allowed.
Note: Auth0 does not have organization information at this step, so users from any of the allowed organizations can register themselves and be added to Auth0.

4. Save and deploy the action.
5. Drag and Drop this trigger in between start and complete.

## **Post Login Triggers**

We need to add the following triggers: **post-social-login-prod** and **post-login-prod**

Step 1: Add **post-social-login-prod** action

1. Navigate to  **Actions → Triggers**.
2. Choose **post-login**.
3. Choose **Add Action → post-social-login-prod**
4. Set the secret keys:

`m2mClientID`:  < Default App Client ID >

`m2mClientSecret`: < Default App Client Secret >

`fulcrumAPIAudience`: < Fulcrum API Identifier >

`insightsServerRegUserEndpoint`: < App User Create API Endpoint >

```json
const axios = require('axios');
/**
* Handler that will be called during the execution of a PostLogin flow.
*
* @param {Event} event - Details about the user and the context in which they are logging in.
* @param {PostLoginAPI} api - Interface whose methods can be used to change the behavior of the login.
*/
const supportedDomains = ["vercel", "levers", "replit", "velotio", "enverus", "setup", "fulcrum"];

exports.onExecutePostLogin = async (event, api) => {
  // For social login, event.connection.strategy === event.connection.name.
  // if(event.connection.strategy !== event.connection.name) {
  //   return
  // }
  const organizationID = event.organization?.id ?? '';
  const organizationTenantID = event.organization?.metadata?.tenant_id ?? '';
  if (!organizationTenantID) {
    const LOG_MESSAGE = "Registration denied"
    const USER_MESSAGE = "Registration denied - The Auth0 organization is missing insights tenant_id in the metadata"
    api.access.deny(LOG_MESSAGE, USER_MESSAGE);
    return;
  }

  const userTenants = event.user.app_metadata['tenant_ids'] ?? [];
  // Convert organizationTenantID to number to ensure type matching
  const organizationTenantIDAsNumber = Number(organizationTenantID);
  const isUserAlreadyCreated = userTenants.includes(organizationTenantIDAsNumber);

  // This flow should not run if the user is already created
  if(isUserAlreadyCreated) {
    return;
  }

  const userEmail = (event.user.email ?? "").toLowerCase();

  // This condition is disabled temporarily to allow cross domain registration
  // const allowRegistration = supportedDomains.some(domain => userEmail.includes(domain)) && userEmail.includes(eventOrganization);
  const allowRegistration = supportedDomains.some(domain => userEmail.includes(domain));
  api.user.setAppMetadata("allow_registration", allowRegistration);

    if (!allowRegistration) {
      const LOG_MESSAGE = "Registration denied"
      const USER_MESSAGE = "Registration denied - You cannot signup using this email address"
      api.access.deny(LOG_MESSAGE, USER_MESSAGE);
      return;
    }

  /** This needs some refinement depending on connections in future */
  const isGoogleProvider = event.connection.strategy !== event.connection.name

  // Step 1: Fetch a token for the M2M application
  const { m2mClientID, m2mClientSecret, fulcrumAPIAudience, insightsServerRegUserEndpoint } = event.secrets
  console.log(insightsServerRegUserEndpoint)
  const m2mTokenCall = {
    method: 'POST',
    url: 'https://leverslabs.us.auth0.com/oauth/token',
    headers: { 'content-type': 'application/json' },
    data: JSON.stringify({
      client_id: m2mClientID,
      client_secret: m2mClientSecret,
      audience: fulcrumAPIAudience,
      grant_type: 'client_credentials'
    })
  };


  let accessToken = '';
  try {
    const { data } = await axios.request(m2mTokenCall);
    accessToken = data.access_token;
  } catch(error) {
    console.error('Error fetching M2M token:', error);
  }

  // Step 2: Send request to backend service to register the user
  try {
    const registerUserCall = {
      method: 'POST',
      url: insightsServerRegUserEndpoint,
      headers: {
        'content-type': 'application/json',
        'Authorization': `Bearer ${accessToken}` // Include M2M token in Authorization header
      },
      data: JSON.stringify({
        "email": event.user.email,
        "external_user_id": `ext_id_${event.user.email}`,
        "name": event.user.email,
        "provider": isGoogleProvider ? "google" : "other",
        "profile_picture": event.user.picture || "",
        "tenant_org_id": event.organization?.id || ""
      })
    };
    console.log('JSON Payload to the register API: ', JSON.stringify({
      "email": event.user.email,
      "external_user_id": `ext_id_${event.user.email}`,
      "name": event.user.email,
      "provider": isGoogleProvider ? "google" : "other",
      "profile_picture": event.user.picture || "",
      "tenant_org_id": event.organization?.id || ""
    }));

    const {data: user} = await axios.request(registerUserCall);
     if (user) {
        api.user.setAppMetadata("insights-app-user-id", user.id);
        api.user.setAppMetadata(`tenant_ids`, user.tenant_ids);
        api.idToken.setCustomClaim('tenant_id', organizationID);
     }
  } catch(error) {
    const LOG_MESSAGE = "Registration denied"
    const USER_MESSAGE = "Registration denied - " + error.message;
    console.error('Error while calling register users endpoint', LOG_MESSAGE, USER_MESSAGE);
    api.access.deny(LOG_MESSAGE, USER_MESSAGE);
  }
};

/**
* Handler that will be invoked when this action is resuming after an external redirect. If your
* onExecutePostLogin function does not perform a redirect, this function can be safely ignored.
*
* @param {Event} event - Details about the user and the context in which they are logging in.
* @param {PostLoginAPI} api - Interface whose methods can be used to change the behavior of the login.
*/
// exports.onContinuePostLogin = async (event, api) => {
// };

```

This Trigger:

- Verifies if the organization has a `tenant_id`in the metadata; denies login if missing.
- Checks if the user is already linked to the organization. This check is done by checking if the `tenant_id` of the organization already belongs to the `tenant_ids` claim of the user, which means the user was already added in the insights backend. If the user is already added, the flow will stop here.
- Validates the user's email domain and denies registration if not allowed.
- Fetches an M2M token and registers the user via the insights backend service.
- On success, updates user metadata and sets a custom `tenant_ids` claim, which is received from the backend response.
5. Save and deploy the action.
6. Drag and Drop this action after start

Step 2: Add **post-login** action

1. In **post-login**.
2. Choose **Add Action → post-login-prod**
3. Set secret keys:

`MNGMT_API_DOMAIN`: [`leverslabs.us.auth0.com`](http://leverslabs.us.auth0.com/)

`MNGMT_API_CLIENT_ID`:< Default APP Client ID >

`MNGMT_API_CLIENT_SECRET`:< Default APP Client Secret >

`MNGMT_API_ROLE`:< Fulcrum Default Role ID >

```json
/**
* Handler that will be called during the execution of a PostLogin flow.
*
* @param {Event} event - Details about the user and the context in which they are logging in.
* @param {PostLoginAPI} api - Interface whose methods can be used to change the behavior of the login.
*/
exports.onExecutePostLogin = async (event, api) => {
  const organizationID = event.organization?.id ?? ''
  const organizationTenantID = event.organization?.metadata?.tenant_id ?? ''
  api.accessToken.setCustomClaim('userId', event.user.app_metadata['insights-app-user-id'])
  api.accessToken.setCustomClaim('tenant_id', Number(organizationTenantID))
  api.idToken.setCustomClaim('tenant_id', Number(organizationTenantID))
  api.idToken.setCustomClaim('organization_name', event.organization?.display_name)
  api.idToken.setCustomClaim('user_roles', event.authorization?.roles)

  if (event.user.user_metadata[`${organizationID}_assigned_permissions`]) return;

  const ManagementClient = require('auth0').ManagementClient;
  const { MNGMT_API_DOMAIN, MNGMT_API_CLIENT_ID, MNGMT_API_CLIENT_SECRET, MNGMT_API_ROLE, AUDIENCE } = event.secrets
  // console.log(MNGMT_API_DOMAIN, MNGMT_API_CLIENT_ID, MNGMT_API_CLIENT_SECRET, MNGMT_API_ROLE, AUDIENCE)
  var management = new ManagementClient({
    domain: MNGMT_API_DOMAIN,
    clientId: MNGMT_API_CLIENT_ID,
    clientSecret: MNGMT_API_CLIENT_SECRET,
    scope: 'update:users', //scope for updating the users
  });

  try {
    const params = { id: event.organization?.id, user_id: event.user.user_id };
    const data = { "roles": [MNGMT_API_ROLE] };
    await management.organizations.addMemberRoles(params, data);
    api.user.setUserMetadata(`${organizationID}_assigned_permissions`, true);
    api.idToken.setCustomClaim('user_roles', [MNGMT_API_ROLE])
  } catch (err) {
    api.user.setUserMetadata("error", err)
  }

}

/**
* Handler that will be invoked when this action is resuming after an external redirect. If your
* onExecutePostLogin function does not perform a redirect, this function can be safely ignored.
*
* @param {Event} event - Details about the user and the context in which they are logging in.
* @param {PostLoginAPI} api - Interface whose methods can be used to change the behavior of the login.
*/
// exports.onContinuePostLogin = async (event, api) => {
// };

```

This trigger:

- Sets custom claims in the `accessToken` and `idToken` for user identification, tenant ID, organization name, and roles.
- Checks if the user has assigned permissions for the organization. If not, proceeds to assign roles.
- Uses the `Auth0 ManagementClient` to assign the user a specific role within the organization.
- Sets a metadata flag indicating that permissions have been assigned to the user.
4. Save and deploy the action.
5. Drag and Drop this action after **post-social-login-prod** step.

# 8. **Troubleshooting**
This section covers some common issues you may encounter during the Auth0 setup and how to resolve them.

#### Issue 1: "Invalid client_id or client_secret" Error
* Problem: When requesting tokens using the client_id and client_secret, you receive an "Invalid client_id or client_secret" error.
* Solution: Double-check the values of your client_id and client_secret in Auth0 Application settings. Make sure you are using the correct credentials associated with the application you're trying to authenticate with.

#### Issue 2: CORS Errors on Frontend
* Problem: You are getting CORS errors when making API calls from your frontend application.
* Solution: Ensure that the allowed origins for your application in the Auth0 dashboard include all the URLs where your frontend is hosted. Go to Applications → The Application → Settings and make sure that the Allowed Web Origins field contains all relevant URLs.

#### Issue 3: "Access Denied" After Successful Login
* Problem: After logging in, users are seeing an "Access Denied" message.
* Solution: Verify if the user has the necessary permissions assigned to their role in Auth0. Also, check if any triggers or rules are restricting access based on conditions (such as the Pre User Registration trigger or Post Login trigger). Ensure the user's metadata and roles are correctly set up.

#### Issue 4: Missing Permissions in Access Token
* Problem: The permissions you configured for your API are not appearing in the access token.
* Solution: Ensure that the "Add Permissions in the Access Token" setting is enabled in the API settings under APIs → Fulcrum API → Settings. Additionally, confirm that the correct scopes are selected when linking the API to your applications.

#### Issue 5: Unable to Create User with Custom Metadata
* Problem: You are unable to assign custom metadata when creating a new user via the API or dashboard.
* Solution: Ensure that the API client has the correct permissions (e.g., update:users) to modify user metadata. Also, check if the custom metadata keys are properly configured and that the user creation process is successful before attempting to modify the metadata.

##### Auth0 setup is successfully complete!, If any issue refer the troubleshooting section.
