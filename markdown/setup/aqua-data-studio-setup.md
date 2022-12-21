### Aqua Data Studio
[Link to product webpage](https://www.aquafold.com/aquadatastudio/).

Aqua Data Studio official [documentation](https://www.aquaclusters.com/app/home/project/public/aquadatastudio/wikibook/Documentation22.3/page/109/Generic-JDBC) for setting up generic JDBC driver

#### Adding the Amazon Timestream JDBC Driver

1. [Download](https://github.com/awslabs/amazon-timestream-driver-jdbc/releases/latest) the Timestream JDBC driver fully shaded JAR file (e.g., `amazon-timestream-jdbc-<version>-shaded.jar`)

2. Launch the Aqua Data Studio application.

3. Access **Server Properties** by using the Keyboard combination "CTRL + SHIFT + P", or follow the steps below. 
    1. Locate the Server in the Server Browser

    2. Right-click on the Server - A Popup Menu Appears

    3. Click on the Item which displays **Server Properties**.

    ![Server Browser](../images/aqua-data-studio/aqua-data-studio-server-browser.png)

4. In the **General** tab, select **Generic - JDBC** and fill in the properties:

    1. In **Name**, enter driver name, e.g., `Timestream JDBC Driver`.

    2. In **URL**, enter the connection string `jdbc:timestream://PropertyName1=value1;PropertyName2=value2...`. For a list of connection properties (e.g., Access Key, Secret Key, Region, etc.), see [README](../../README.md#optional-connection-properties).

    3. In **Driver**, enter `software.amazon.timestream.jdbc.TimestreamDriver`. 

    4. In **Driver Location**, click the “Browse” on the right side to select the Timestream JDBC JAR file.

    5. Click on **Test Connection** (button on the lower right) to ensure a connection can be established.

    6. Click **Save** (button on the lower left) to save the configuration.

![Example](../images/aqua-data-studio/aqua-data-studio-sign-in.png)

#### Connecting to the database

1. After the Amazon Timestream JDBC driver is set up, you should see “Timestream JDBC Driver“ under “Local Database Servers“ like below image

![Database](../images/aqua-data-studio/aqua-data-studio-server.png)

2. Double click on “Timestream JDBC Driver“ to connect to the database. A pop-up window will occur and ask for authentication. Press **OK** without entering any field since Timestream authentication can be set through AWS CLI configuration or passed through the connection string (as entered in the URL field).

![Log in](../images/aqua-data-studio/aqua-data-studio-pop-up.png)

3. Click on the “+” beside **Database** to expand, and click “+“ to expand on **\<DEFAULT\>**, then click on “+” beside **Schemas** to see the list of databases.

![Databases](../images/aqua-data-studio/aqua-data-studio-databases.png)

4. To access the list of tables under desired database, click on the “+” beside that database and then click on “+” beside **Table**.

![Tables](../images/aqua-data-studio/aqua-data-studio-tables.png)

#### Known Limitations
1. Free version of Aqua Data Studio is unable to filter tables with generic JDBC driver connection.
2. To load data and join tables with free version of Aqua Data Studio, user needs to manually enter SQL queries by using [**Query Analyzer**](https://www.aquaclusters.com/app/home/project/public/aquadatastudio/wikibook/Documentation22.3/page/145/Query-Analyzer-SQL-Tool) (Ctrl + Q).
