# Monitor Progress in EMR Cluster

There are two methods provided by AWS to track progress of EMR clusters: through the Spark Web UIs interface, and through cluster log files. 

Spark Web UIs is the most intuitive way for monitoring spark application by visualizing scheduler stages and tasks, and information about running executors. It also provides a summary of RDD sizes and memory usage, and more detailed environmental information than in EMR console. 

Cluster log files are more useful in identifying issues with Spark processes. It is also applicable in our project to check the general stage information since we wrote logs through our python codes.

* <a href = "#sparkUI">Use Spark Web UIs (recommend)</a>
  * <a href = "#sWin">Windows System</a>
  * <a href = "#sLinux">Linux System</a>
* <a href = "#log">Through cluster log files</a>
* <a href = "#appendix">Appendix: Web Interfaces Hosted on Amazon EMR Clusters</a>>

&emsp;

## <h2 id = "sparkUI">Access Spark Web UIs</h2>

&emsp;

### <h3 id = "sWin"> Windows</h3>

​	<a href = "#Win_step1">Step 1: Install PuTTY and convert private key</a>

​	<a href = "#Win_step2">Step 2: Install FoxyProxy (Chrome)</a>

​	<a href = "#Win_step3">Step 3: Find an available port</a>

​	<a href = "#Win_step4">Step 4: Set up SSH Tunnel</a>

​	<a href = "#Win_step5">Step 5: Open Spark Web UIs in browser</a>

#### <h4 id = "Win_step1">Step1: Install PuTTY and convert private key</h4>

1. Download and install latest PuTTY from <a href = "https://www.chiark.greenend.org.uk/~sgtatham/putty/latest.html">PuTTY packages download page</a>

2. In windows search box, type PuTTYgen and open the software.

   <img src="image\1568929926893.png" alt="1568929926893" style="zoom:80%;" />

3. Click **Load** button, select the option to display files of all types. Select the .pem file for the key pair that you specified when launching the instance, and choose **Open**. Click **OK**.

4. Under **Type of key to generate**, choose **RSA**.

5. Click **Save private key**,. PuTTYgen will display a warning about saving the key without passphrase. Choose **Yes** 

6. Specify the key file name. The new key file for PuTTY ends with .ppk extension.

   

#### <h4 id = "Win_step2">Step 2: Install FoxyProxy (Chrome)</h4>

1. Open Chrome browser and add the FoxyProxy Standard extension from <a href = "https://chrome.google.com/webstore/search/foxy proxy">Chrome web store</a>

2. Use a text editor to create a file named *foxyproxy-settings.xml* with the following contents:

   ```xml
   <?xml version="1.0" encoding="UTF-8"?>
   <foxyproxy>
      <proxies>
         <proxy name="emr-socks-proxy" id="2322596116" notes="" fromSubscription="false" enabled="true" mode="manual" selectedTabIndex="2" lastresort="false" animatedIcons="true" includeInCycle="true" color="#0055E5" proxyDNS="true" noInternalIPs="false" autoconfMode="pac" clearCacheBeforeUse="false" disableCache="false" clearCookiesBeforeUse="false" rejectCookies="false">
            <matches>
               <match enabled="true" name="*ec2*.amazonaws.com*" pattern="*ec2*.amazonaws.com*" isRegEx="false" isBlackList="false" isMultiLine="false" caseSensitive="false" fromSubscription="false" />
               <match enabled="true" name="*ec2*.compute*" pattern="*ec2*.compute*" isRegEx="false" isBlackList="false" isMultiLine="false" caseSensitive="false" fromSubscription="false" />
               <match enabled="true" name="10.*" pattern="http://10.*" isRegEx="false" isBlackList="false" isMultiLine="false" caseSensitive="false" fromSubscription="false" />
               <match enabled="true" name="*10*.amazonaws.com*" pattern="*10*.amazonaws.com*" isRegEx="false" isBlackList="false" isMultiLine="false" caseSensitive="false" fromSubscription="false" />
               <match enabled="true" name="*10*.compute*" pattern="*10*.compute*" isRegEx="false" isBlackList="false" isMultiLine="false" caseSensitive="false" fromSubscription="false" /> 
               <match enabled="true" name="*.compute.internal*" pattern="*.compute.internal*" isRegEx="false" isBlackList="false" isMultiLine="false" caseSensitive="false" fromSubscription="false"/>
               <match enabled="true" name="*.ec2.internal* " pattern="*.ec2.internal*" isRegEx="false" isBlackList="false" isMultiLine="false" caseSensitive="false" fromSubscription="false"/>	  
   	   </matches>
            <manualconf host="localhost" port="8157" socksversion="5" isSocks="true" username="" password="" domain="" />
         </proxy>
      </proxies>
   </foxyproxy>
   ```

3. On the Chrome tool bar, choose FoxyProxy extension <img src="image\1568931546210.png" alt="1568931546210"/>. In the pull-down menu, choose **Options**

4. On the FoxyProxy page, choose **Import/Export**. Then choose **Choose File**, find and select the created *foxyproxy-settings.xml*, and choose **Open**. 

5. In the prompted window of overwriting existing settings, choose **Replace**.

6. In **Proxy mode**, choose **Use proxies based on their pre-defined patterns and priorities**

   <img src="image\1568931888466.png" alt="1568931888466" style="zoom:80%;" />



#### <h4 id = "Win_step3">Step3: Find an available port

1. Open cmd as administrator, type the command below to get a list of occupied port. 

   ```shell
   netstat -an
   ```

2. Infer the available port number. Usually port over 10000 are available. Then type the following command to confirm the inferred number is not in use. If the port is available, this command will get nothing. Please replace *<span style = "color:#006400; font-style:italic" >8157</span>* with an unused port number

   ```shell
   netstat -a|findstr 8157
   ```

    

#### <h4 id = "Win_step4">Step 4: Set up SSH Tunnel</h4>

1. Open **PuTTY**.
2. In the **Category** list, choose **Session**. In the **Host Name** field, type the master public DNS names. You can find it on the cluster **Summary** tab in the EMR console. Make sure the **Port** field is 22.

   <img src="image\1568988197519.png" alt="1568988197519" style="zoom:80%;" />

3. In the **Category** list, expand **Connection** > **SSH**, and then choose **Auth**. In the **Private key file for authentication** field, click **Browse**, and open the .ppk file you generated at step 1.

   <img src="image\1568988642761.png" alt="1568988642761" style="zoom:80%;" />

4. Still under the list of **SSH**, choose **Tunnels**. 

5. In the **Source port** field, type the available port number you find at step 3. Leave **Destination** field empty, and choose options of **Dynamic** and **Auto**. Click **Add**. The port number with a prefix D will be added to the **Forwarded ports** field.

   <img src="image\1568989031255.png" alt="1568989031255" style="zoom:80%;" />

6. Click **Open**. Choose **Yes** to dismiss the PuTTY security alert.

   

#### <h4 id = "Win_step5">Step 5: Open Spark Web UIs in browser</h4>

1. Open a browser.

2. Choose FoxyProxy extension <img src="image\1568931546210.png" alt="1568931546210"/>in the Chrome toolbar.  In the pull-down menu, choose **Options**.

3. On the FoxyProxy page, choose **Proxies**. 

   <img src="image\1569005574163.png" alt="1569005574163" style="zoom:80%;" />

4. Double click the non-default proxy. In the prompt up window, choose the **Proxy Details** tab, enter the unused port number you get at Step 3 in **Port** field. Click **Save**.

   <img src="image\1569007438207.png" alt="1569007438207" style="zoom:80%;" />

5. In the address bar, enter `http://master-public-dns-name:18080/`. Be sure to replace the `master-public-dns-name`with the public dns name of your cluster. You can access all web interface provided by AWS listed in the <a href = "#appendix"> Appendix</a>

   <img src="image\1568989807431.png" alt="1568989807431" style="zoom:80%;" />

&emsp;

### <h3 id = "sLinux">Linux</h3>

​	<a href = "#Linux_step1">Step 1: Find an unused  port</a>

​	<a href = "#Linux_step2">Step 2: Set Up an SSH Tunnel to the Master Node using local port forwarding</a>

#### <h4 id = "Linux_step1">Step 1: Find an unused port</h4>

1. Open a terminal window

2. Type the following command to check occupied port, and infer the available port number. Usually port number over 10000 are available.

   ```bash
   netstat -a
   ```

3. Type the following command  to confirm the inferred number is not in use. If the port is available, this command will print nothing. Please replace *<span style = "color:#006400; font-style:italic" >8157</span>* with an unused port number

   ```bash
   sudo netstat -ap|grep 8157
   ```



#### <h4 id = "Linux_step2">Step 2: Set up an SSH tunnel to the master node using local port forwarding</h4>

1. In the terminal window, type the following command. Please replace *<span style = "color:#006400; font-style:italic" >8157</span>* with an unused port number, and *<span style = "color: #b5651d; font-style: italic">ec2-###-##-##-###.compute-1.amazonaws.com</span>* with the master public DNS name of the cluster. 

   ```bash
   ssh -i ~/mykeypair.pem -N -L 8157:ec2-###-##-##-###.compute-1.amazonaws.com:18080 hadoop@ec2-###-##-##-###.compute-1.amazonaws.com
   ```

2. Open a browser and enter `localhost:8175` in the address bar. Don't forget to replace the `8175` with an unused port number in your computer.

&emsp;

&emsp;

## <h2 id ="log">Access cluster log files</h2>

<a href = "#log_step1">Step 1: Open EMR console for the cluster</a>

<a href = "#log_step2">Step 2: Browse stderr and stdout</a>

#### <h4 id = "log_step1">Step 1: Open EMR console for the cluster</h4>

1. Open the EMR console of your cluster

2. Choose the **Steps** tab. There will be one record for each step you set up in emr.tf of terraform.

3. Click *<span style = "color:blue">view logs</span>* in the **Log files** columns of the step you want to check. There are three available files: *<span style = "color:blue">controller</span>*, *<span style = "color:blue">stderr</span>*, and *<span style = "color:blue">stdout</span>*

   <img src="image\1568997664852.png" alt="1568997664852"/>

   

#### <h4 id = "log_step2">Step 2: Browse stderr and stdout</h4>

​	There are  two log files I would recommend to read during or after the cluster is running: *<span style = "color:blue">stderr</span>* and *<span style = "color:blue">stdout</span>*. You should check *<span style = "color:blue">controller</span>* only if you are not confident about your step settings in *emr.tf*. 

​	Standard error (*<span style = "color:blue">stderr</span>*),  is the default file for writing a process's output, and the last error before a cluster is terminated (if there's any). 

​	As for the standard out (*<span style = "color:blue">stdout</span>*) file, there are three keywords that worth a check: 

​	1) *Error*. 

​	2) *WARN*. Spark memory issue is often written as a warning message but not errors in log files. If you see consecutive messages with keywords like **"lost task # in stage # "** or **"ExecutorLostFailure"**,  there's no 	need to wait for the last task in the stage to lose. Just terminate the cluster. However, if you see any 	warning messages with `java.util.NoSuchElementException: None.get`, ignore it. It is the problem from EMR version and wouldn't hurt your process.

​	3) *WARN Main*. This kind of record is added by using `logger.warn()` in python code. In our case, there are 8 stages  in run_geopyspark.py. Their starting time and elapsed time are written as the main warning log.

&emsp;

&emsp;

## <h2 id = "appendix">Appendix: Web Interfaces on Amazon EMR Clusters</h2>

| Name of interface    | URL                                                          |
| -------------------- | ------------------------------------------------------------ |
| Spark Web UIs        | http://*<span style = "color: red; font-style: italic">master-public-dns-name</span>*:18080/ |
| Ganglia              | http://*<span style = "color: red; font-style: italic">master-public-dns-name</span>*/ganglia/ |
| YARN ResourceManager | http://*<span style = "color: red; font-style: italic">master-public-dns-name</span>*:8088/ |
| YARN NodeManager     | http://*<span style = "color: red; font-style: italic">master-public-dns-name</span>*:8042/ |
| Hadoop HDFS NameNode | http://*<span style = "color: red; font-style: italic">master-public-dns-name</span>*:50070/ |
| Hadoop HDFS DataNode | http://*<span style = "color: red; font-style: italic">master-public-dns-name</span>*:50075/ |
| Zeppelin             | http://*<span style = "color: red; font-style: italic">master-public-dns-name</span>*:8890/ |
| Hue                  | http://<span style = "color: red; font-style: italic">master-public-dns-name</span>:8888/ |
| HBase                | http://*<span style = "color: red; font-style: italic">master-public-dns-name</span>*:16010/ |
| JupyterHub           | https://*<span style = "color: red; font-style: italic">master-public-dns-name</span>*:9443/ |

(source: <a href = "https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-web-interfaces.html">View Web Interfaces Hosted on Amazon EMR Clusters</a> )



