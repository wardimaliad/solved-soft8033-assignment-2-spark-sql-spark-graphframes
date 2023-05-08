Download Link: https://assignmentchef.com/product/solved-soft8033-assignment-2-spark-sql-spark-graphframes
<br>






<strong><u>BACKGROUND.</u> </strong>

It’s been already a few weeks since you started your short-term internship in the Data Analytics Department of the start-up OptimiseYourJourney, which will enter the market next year with a clear goal in mind: “<em>leverage Big Data technologies for improving the user experience in transportation”</em>. Your contribution in Assignment 1 has proven the potential OptimiseYourJourney can obtain by applying MapReduce to analyse large-scale public transportation datasets as the one in the New York City Bike Sharing System: <u>https://www.citibikenyc.com/</u>




In the department meeting that has just finished your boss was particularly happy, again.

<ul>

 <li>The very same dataset from Assignment 1 (let’s call it my_dataset_1) provides an opportunity to leverage other large-scale data analysis libraries, such as Spark SQL.</li>

 <li>The graph structure of the dataset allows you to explore the potential of Spark GraphFrames, a small library of Spark specialised in the parallel execution of classical graph algorithms. To do so, two small graph examples (let’s call them my_dataset_2 and my_dataset_3) are provided to explore the classical algorithms of: oDijkstra – for finding the shortest path from a source node to the remaining nodes.</li>

</ul>

o PageRank – for assigning a value to each node based on its neighbourhood.













<strong><u>DATASET 1:</u>  </strong>

This dataset occupies ~80MB and contains 73 files. Each file contains all the trips registered the CitiBike system for a concrete day:

<ul>

 <li>csv =&gt; All trips registered on the 1<sup>st</sup> of May of 2019.</li>

 <li>csv =&gt; All trips registered on the 2<sup>nd</sup> of May of 2019.</li>

 <li>…</li>

 <li>csv =&gt; All trips registered on the 12<sup>th</sup> of July of 2019.</li>

</ul>




Altogether, the files contain 444,110 rows. Each row contains the following fields:

<strong><em>start_time , stop_time , trip_duration , start_station_id , start_station_name , start_station_latitude , start_station_longitude , stop_station_id , stop_station_name , stop_station_latitude , stop_station_longitude , bike_id , user_type , birth_year , gender , trip_id </em></strong>

<strong><em> </em></strong>

<ul>

 <li><strong>(00) <em>start_time</em></strong></li>

</ul>

! A String representing the time the trip started at.

&lt;%Y/%m/%d %H:%M:%S&gt;

!Example: “2019/05/02 10:05:00”

<ul>

 <li><strong>(01) <em>stop_time</em></strong></li>

</ul>

! A String representing the time the trip finished at.

&lt;%Y/%m/%d %H:%M:%S&gt;

!Example: “2019/05/02 10:10:00”

<ul>

 <li><strong>(02) <em>trip_duration</em></strong></li>

</ul>

!An Integer representing the duration of the trip.

!Example: 300

<ul>

 <li><strong>(03) <em>start_station_id</em></strong></li>

</ul>

!An Integer representing the ID of the CityBike station the trip started from.

!Example: 150

<ul>

 <li><strong>(04) <em>start_station_name</em></strong></li>

</ul>

!A String representing the name of the CitiBike station the trip started from.

!Example: “E 2 St &amp;; Avenue C”.

<ul>

 <li><strong>(05) <em>start_station_latitude</em></strong></li>

</ul>

!A Float representing the latitude of the CitiBike station the trip started from.

!Example: 40.7208736

<ul>

 <li><strong>(06) <em>start_station_longitude</em></strong></li>

</ul>

!A Float representing the longitude of the CitiBike station the trip started from.

! Example:  -73.98085795







<ul>

 <li><strong>(07) <em>stop_station_id</em></strong></li>

</ul>

!An Integer representing the ID of the CityBike station the trip stopped at.

!Example: 150

<ul>

 <li><strong>(08) <em>stop_station_name</em></strong></li>

</ul>

! A String representing the name of the CitiBike station the trip stopped at.

!Example: “E 2 St &amp;; Avenue C”.

<ul>

 <li><strong>(09) <em>stop_station_latitude</em></strong></li>

</ul>

!A Float representing the latitude of the CitiBike station the trip stopped at.

!Example: 40.7208736

<ul>

 <li><strong>(10) <em>stop_station_longitude</em></strong></li>

</ul>

!A Float representing the longitude of the CitiBike station the trip stopped at.

! Example:  -73.98085795

<ul>

 <li><strong>(11) <em>bike_id</em></strong></li>

</ul>

! An Integer representing the id of the bike used in the trip.

! Example:  33882.

<ul>

 <li><strong>(12) <em>user_type</em></strong></li>

</ul>

! A String representing the type of user using the bike (it can be either “Subscriber” or “Customer”).

!Example: “Subscriber”.

<ul>

 <li><strong>(13) <em>birth_year</em></strong></li>

</ul>

! An Integer representing the birth year of the user using the bike.

! Example:  1990.

<ul>

 <li><strong>(14) <em>gender</em></strong></li>

</ul>

! An Integer representing the gender of the user using the bike (it can be either 0 =&gt; Unknown; 1 =&gt; male; 2 =&gt; female).

! Example:  2.

<ul>

 <li><strong>(15) <em>trip_id</em></strong></li>

</ul>

! An Integer representing the id of the trip.

! Example:  190.

<strong> </strong>

<strong> </strong>

<strong> </strong>

<strong><u>DATASET 2:</u>  </strong>

<strong> </strong>This dataset consists in the file tiny_graph.txt, which contains 26 edges (indeed, 13 edges, one on each direction) in a graph with 8 nodes.

<strong><u>DATASET 3:</u>  </strong>

<strong> </strong>This dataset consists in the file tiny_graph.txt, which contains 18 edges (indeed, 9 edges, one on each direction) in a graph with 6 nodes.

<strong> </strong>

<strong> </strong>

<strong> </strong>

<strong> </strong>

<strong> </strong>

<strong> </strong>

<strong> </strong>

<strong><u>TASKS / EXERCISES.</u></strong>

The tasks / exercises to be completed as part of the assignment are described in the next pages:




<ul>

 <li>The following exercises are placed in the folder <strong>my_code:</strong>

  <ol>

   <li><strong>A02_Part1/</strong>py</li>

   <li><strong>A02_Part2/</strong>py  3. <strong>A02_Part3/</strong>A02_Part3.py</li>

   <li><strong>A02_Part4/</strong>py</li>

   <li><strong>A02_Part5/</strong>py</li>

  </ol></li>

</ul>




<strong>Marks are as follows: </strong>

<ol>

 <li><strong>A02_Part1/</strong>py =&gt; 18 marks</li>

 <li><strong>A02_Part2/</strong>py =&gt; 18 marks 3. <strong>A02_Part3/</strong>A02_Part3.py =&gt; 18 marks</li>

 <li><strong>A02_Part4/</strong>py =&gt; 18 marks</li>

 <li><strong>A02_Part5/</strong>py =&gt; 28 marks</li>

</ol>







<strong>Tasks: </strong>

<ol>

 <li><strong>A02_Part1/</strong>py</li>

 <li><strong>A02_Part2/</strong>py</li>

 <li><strong>A02_Part3/</strong>py</li>

</ol>

<u>Complete the function </u><strong><u>my_main</u></strong><u> of the Python program.</u>

Do not modify the name of the function nor the parameters it receives.

The entire work must be done within Spark SQL:

<ul>

 <li>The function my_main must start with the creation operation ‘read’ above loading the dataset to Spark SQL.</li>

 <li>The function my_main must finish with an action operation ‘collect’, gathering and printing by the screen the result of the Spark SQL job.</li>

 <li>The function my_main must not contain any other action operation ‘collect’ other than the one appearing at the very end of the function.</li>

 <li>The resVAL iterator returned by ‘collect’ must be printed straight away, you cannot edit it to alter its format for printing.</li>

</ul>




<ol start="4">

 <li><strong>A02_Part4/</strong>A02_Part4.py</li>

</ol>

<u>Complete the function </u><strong><u>compute_page_rank</u></strong><u> of the Python program.</u>

Do not modify the name of the function nor the parameters it receives.

The function must return a dictionary with (key, value) pairs, where:

<ul>

 <li>Each key represents a node id.</li>

 <li>Each value represents the pagerank value computed for this node id.</li>

</ul>




<ol start="5">

 <li><strong>A02_Part5/</strong>A02_Part5.py</li>

</ol>

<u>Complete the function </u><strong><u>my_main</u></strong><u> of the Python program.</u>

Do not modify the name of the function nor the parameters it receives.

The entire work must be done within Spark SQL:

<ul>

 <li>The function my_main must start with the creation operation ‘read’ above loading the dataset to Spark SQL.</li>

 <li>The function my_main must finish with an action operation ‘collect’, gathering and printing by the screen the result of the Spark SQL job.</li>

 <li>The function my_main must not contain any other action operation ‘collect’ other than the one appearing at the very end of the function.</li>

 <li>The resVAL iterator returned by ‘collect’ must be printed straight away, you cannot edit it to alter its format for printing.</li>

</ul>

<strong> </strong>