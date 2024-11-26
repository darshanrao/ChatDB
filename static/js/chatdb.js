// script.js
const sendBtn = document.getElementById('send-btn');
const userInput = document.getElementById('user-input');
const messages = document.getElementById('messages');
const mysqlTab = document.getElementById('mysqlTab');
const mongodbTab = document.getElementById('mongodbTab');

let activeTab = 'mysql'; 
let currDatabase = 'school';

function switchTab(selectedTab) {
    if (selectedTab === 'mysql') {
        mysqlTab.classList.add('active');
        mongodbTab.classList.remove('active');
        messages.innerText = '';
        activeTab = 'mysql';
    } else if (selectedTab === 'mongodb') {
        mongodbTab.classList.add('active');
        mysqlTab.classList.remove('active');
        messages.innerText = '';
        activeTab = 'mongodb';
    }
}

mysqlTab.addEventListener('click', () => switchTab('mysql'));
mongodbTab.addEventListener('click', () => switchTab('mongodb'));


sendBtn.addEventListener('click', () => {
  const query = userInput.value.trim();
  if (!query) return;

  const useDatabaseRegex = /^USE DATABASE\s+['"]?([\w-]+)['"]?;?$/i;
  const match = query.match(useDatabaseRegex);

  const sampleQueryRegex = /sample queries(?: for)?\s+(.*)/i;
  const match2 = query.match(sampleQueryRegex);

  if (match) {
      addMessage(query, 'user-message');
      currDatabase = match[1]; 
      addMessage(`Switched to database: ${currDatabase}`, 'bot-message');
      return; 
  }

  else if (match2) {
    const operation = match2[1].trim().toLowerCase(); 
    console.log(`Detected operation: ${operation}`);
    addMessage(query, 'user-message');
    fetch("/api/sample-queries", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ operation: operation, db: activeTab })
    })
        .then(response => response.json())
        .then(data => {
          console.log("response recieved", data);
            const exampleQueries = data.queries;
            exampleQueries.forEach(({ description, sql }) => {
                addMessage(`Description: ${description} \n Query: ${sql}`, "bot-message");
            });
        })
        .catch(err => addMessage("Error: Could not fetch sample queries.", "bot-message"));
    }

  else {
    try {        
        addMessage(query, 'user-message');
        // userInput.value = '';
        if (activeTab == 'mysql') {
        fetch('/api/query-mysql', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({"db_name": currDatabase, "query": query})
        })
      
        .then(response => response.json())
        .then(data => {
            addMessage( data.query, 'bot-message');
            displayTable(data.results);
        })
        .catch(err => addMessage('Error: Could not fetch response.', 'bot-message'));
      }
      else {
        fetch('/api/query-mongodb', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({"db_name": currDatabase, "query": query})
        })
      
        .then(response => response.json())
        .then(data => {
            addMessage(data.query, 'bot-message');
            displayTable(data.results);
        })
        .catch(err => addMessage('Error: Could not fetch response.', 'bot-message'));
      }
    } catch (err) {
        addMessage('Error: Please provide a valid JSON query format.', 'bot-message');
    }
  }
  userInput.value = '';

});

function addMessage(text, className) {
    const msg = document.createElement('div');
    msg.textContent = text;
    msg.classList.add('message', className);
    messages.appendChild(msg);
    messages.scrollTop = messages.scrollHeight;
}


function displayTable(data) {
  const resultsDiv = document.getElementById('results');
  resultsDiv.innerHTML = '';

  if (data.length === 0) {
    resultsDiv.innerHTML = '<p>No results found.</p>';
    return;
  }

  const table = document.createElement('table');
  const thead = document.createElement('thead');
  const tbody = document.createElement('tbody');

  const headers = Object.keys(data[0]);
  const headerRow = document.createElement('tr');
  headers.forEach(header => {
    const th = document.createElement('th');
    th.textContent = header;
    headerRow.appendChild(th);
  });
  thead.appendChild(headerRow);

  data.forEach(row => {
    const tr = document.createElement('tr');
    headers.forEach(header => {
      const td = document.createElement('td');
      td.textContent = row[header];
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  });

  table.appendChild(thead);
  table.appendChild(tbody);
  resultsDiv.appendChild(table);
}

const uploadBtn = document.getElementById('upload-btn');
const uploadFileInput = document.getElementById('upload-file');

uploadBtn.addEventListener('click', () => {
  uploadFileInput.click();
});

uploadFileInput.setAttribute('webkitdirectory', '');

uploadFileInput.addEventListener('change', async () => {
  const files = Array.from(uploadFileInput.files);
  if (!files.length) return;

  const folderPath = files[0].webkitRelativePath;
  const dbName = folderPath.split('/')[0]; 
  
  const formData = new FormData();
  formData.append('db_name', dbName);

  let csvCount = 0;
  files.forEach(file => {
    if (file.name.endsWith('.csv')) {
      formData.append('files', file);
      csvCount++;
    } else {
      addMessage(`Skipping ${file.name}: Not a CSV file`, 'bot-message');
    }
  });

  if (csvCount === 0) {
    addMessage('No CSV files found in the selected folder', 'bot-message');
    return;
  }

  addMessage(`Processing folder "${dbName}" with ${csvCount} CSV files...`, 'bot-message');

  try {
    if (activeTab == 'mongodb') {
      const response = await fetch('api/upload-mongodb', {
        method: 'POST',
        body: formData,
      });
      
      if (!response.ok) {
        throw new Error('Failed to upload files');
      }

      const data = await response.json();
      addMessage(data.message || `Successfully uploaded ${csvCount} tables to database "${dbName}"`, 'bot-message');
    }

    else {
      const response = await fetch('api/upload-mysql', {
        method: 'POST',
        body: formData,
        });
      
      if (!response.ok) {
        throw new Error('Failed to upload files');
      }

      const data = await response.json();
      addMessage(data.message || `Successfully uploaded ${csvCount} tables to database "${dbName}"`, 'bot-message');

  }
  } catch (err) {
    addMessage(`Error: Could not upload tables to database "${dbName}".`, 'bot-message');
    console.error(err);
  }

});