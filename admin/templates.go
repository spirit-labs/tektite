// Copyright 2024 The Tektite Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package admin

var homeTemplate = `<html>
<head>
<link href='https://fonts.googleapis.com/css?family=Roboto:400' rel='stylesheet' type='text/css'>
<style>body {font-family: 'Roboto', sans-serif;}</style>
<title>Tektite server</title>
</head>
<body>
<h1>Tektite Server</h1>
Date/time: {{.Date}}<br></br>
Memory Usage: {{.Memory}} MB<br></br>
Up-time: {{.Uptime}}<br></br>
<ul>
<li><a href="topics">Topics</a></li>
<li><a href="streams">Streams</a></li>
<li><a href="database">Database stats</a></li>
<li><a href="config">View server config</a></li>
<li><a href="cluster">View cluster information</a></li>
<ul>
</body>
</html>
`

var databaseTemplate = `<html>
<head>
<link href='https://fonts.googleapis.com/css?family=Roboto:400' rel='stylesheet' type='text/css'>
<style>body {font-family: 'Roboto', sans-serif;}</style>
<title>Database Stats</title>
</head>
<body>
<h1>Database Stats</h1>

<table border="1" width="50%">
<tr>
	<th width="35%">Total size</th>
	<th>Total table count</th>
	<th>Total entry count</th>
</tr>
<tr>
	<td>{{format_bytes .LevelMgrStats.TotBytes}}</td>
	<td>{{.LevelMgrStats.TotTables}}</td>
	<td>{{.LevelMgrStats.TotEntries}}</td>
</tr>
</table>

<br></br>

<table border="1" width="50%">
<tr>
	<th width="35%">Total bytes in</th>
	<th>Total tables in</th>
	<th>Total entries in</th>
</tr>
<tr>
	<td>{{format_bytes .LevelMgrStats.BytesIn}}</td>
	<td>{{.LevelMgrStats.TablesIn}}</td>
	<td>{{.LevelMgrStats.EntriesIn}}</td>
</tr>
</table>

<br></br>

<table border="1" width="50%">
<tr>
	<th>Level</th>
	<th>Size</th>
	<th>Table count</th>
	<th>Entry count</th>
</tr>
{{range $level, $value := .LevelStats}}
<tr>
	<td>{{$level}}</td>
	<td>{{format_bytes $value.Bytes}}</td>
	<td>{{$value.Tables}}</td>
	<td>{{$value.Entries}}</td>
</tr>
{{end}}
</table>

</body>
</html>
`
var topicsTemplate = `<html>
<head>
<link href='https://fonts.googleapis.com/css?family=Roboto:400' rel='stylesheet' type='text/css'>
<style>body {font-family: 'Roboto', sans-serif;}</style>
<title>Tektite Topics</title>
</head>
<body>
<h1>Topics</h1>

<table border="1" width="50%">
<tr>
	<th>Name</th>
	<th>Partitions</th>
	<th>Read/Write</th>
</tr>
{{range .}}
<tr>
	<td>{{.Name}}</td>
	<td>{{.Partitions}}</td>
	<td>{{.RW}}</td>
</tr>
{{end}}
</table>

</body>
</html>
`

var streamsTemplate = `<html>
<head>
<link href='https://fonts.googleapis.com/css?family=Roboto:400' rel='stylesheet' type='text/css'>
<style>body {font-family: 'Roboto', sans-serif;}</style>
<title>Tektite Streams</title>
</head>
<body>
<h1>Streams</h1>
<table border="1" width="100%">
<tr>
	<th>Name</th>
	<th width="15%">Definition</th>
	<th width="15%">In Schema</th>
	<th>In Partitions</th>
	<th>In Mapping</th>
	<th width="15%">Out Schema</th>
	<th>Out Partitions</th>
	<th>Out Mapping</th>
	<th>Child Streams</th>
</tr>
{{range .}}
<tr>
	<td>{{.Name}}</td>
	<td>{{.StreamDef}}</td>
	<td>{{.InSchema}}</td>
	<td>{{.InPartitions}}</td>
	<td>{{.InMapping}}</td>
	<td>{{.OutSchema}}</td>
	<td>{{.OutPartitions}}</td>
	<td>{{.OutMapping}}</td>
	<td>{{.ChildStreams}}</td>
</tr>
{{end}}
</table>
</body>
</html>
`

var configTemplate = `<html>
<head>
<link href='https://fonts.googleapis.com/css?family=Roboto:400' rel='stylesheet' type='text/css'>
<style>body {font-family: 'Roboto', sans-serif;} pre {font-family: 'Consolas', monospace;white-space: pre-wrap;}</style>
<title>Tektite Config</title>
</head>
<body>
<h1>Config</h1>
<pre>{{.}}</pre>
</body>
</html>
`

var clusterTemplate = `<html>
<head>
<link href='https://fonts.googleapis.com/css?family=Roboto:400' rel='stylesheet' type='text/css'>
<style>body {font-family: 'Roboto', sans-serif;} pre {font-family: 'Consolas', monospace;white-space: pre-wrap;}</style>
<title>Tektite Cluster State</title>
</head>
<body>
<h1>Cluster State</h1>
Node count: {{.NodeCount}}<br></br>
Live nodes: {{.LiveNodes}}<br></br>
<br></br>
{{range $nodeID, $nodeData := .NodesData}}
	<h3>Node id: {{$nodeData.NodeID}}</h3><br></br>
	Leader count: {{$nodeData.LeaderCount}}<br></br>
	Replica Count: {{$nodeData.ReplicaCount}}<br></br>
	<table border="1" width="100%">
		<tr>
			<th>Processor ID</th>
			<th>Live?</th>
			<th>Backup?</th>
			<th>Synced?</th>
		</tr>
		{{range $processorID, $processorInfo := $nodeData.Processors}}
			<tr>
				<td>{{$processorID}}</td>
				<td>{{$processorInfo.Leader}}</td>
				<td>{{$processorInfo.Backup}}</td>
				<td>{{$processorInfo.Synced}}</td>
			</tr>
		{{end}}
	</table>
{{end}}
</body>
</html>`
