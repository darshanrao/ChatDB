// script.js
const sendBtn = document.getElementById('send-btn');
const userInput = document.getElementById('user-input');
const messages = document.getElementById('messages');
const mysqlTab = document.getElementById('mysqlTab');
const mongodbTab = document.getElementById('mongodbTab');

let activeTab = 'mysql'; 

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

// Add event listeners for tab clicks
mysqlTab.addEventListener('click', () => switchTab('mysql'));
mongodbTab.addEventListener('click', () => switchTab('mongodb'));


sendBtn.addEventListener('click', () => {
  const query = userInput.value.trim();
  if (!query) return;

  try {
      // Parse the input as sJSON
      const queryData = JSON.parse(query);
      
      addMessage(query, 'user-message');
      userInput.value = '';
      if (activeTab == 'mysql') {
      // Call Flask backend with MongoDB query endpoint
      fetch('/api/query-mysql', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(queryData)
      })
    
      .then(response => response.json())
      .then(data => {
          // addMessage(data.response, 'bot-message'); 
          addMessage("Query Executed", 'bot-message');
          displayTable(data.results);
      })
      .catch(err => addMessage('Error: Could not fetch response.', 'bot-message'));
    }
    else {
      fetch('/api/query-mongodb', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(queryData)
      })
    
      .then(response => response.json())
      .then(data => {
          addMessage(data.response, 'bot-message'); 
          addMessage("Query Executed", 'bot-message');
          displayTable(data.results);
      })
      .catch(err => addMessage('Error: Could not fetch response.', 'bot-message'));
    }
  } catch (err) {
      addMessage('Error: Please provide a valid JSON query format.', 'bot-message');
  }
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

  // Create table
  const table = document.createElement('table');
  const thead = document.createElement('thead');
  const tbody = document.createElement('tbody');

  // Add table headers
  const headers = Object.keys(data[0]);
  const headerRow = document.createElement('tr');
  headers.forEach(header => {
    const th = document.createElement('th');
    th.textContent = header;
    headerRow.appendChild(th);
  });
  thead.appendChild(headerRow);

  // Add table rows
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

// Only allow directory selection
uploadFileInput.setAttribute('webkitdirectory', '');

uploadFileInput.addEventListener('change', async () => {
  const files = Array.from(uploadFileInput.files);
  if (!files.length) return;

  // Get folder name from the first file's path
  const folderPath = files[0].webkitRelativePath;
  const dbName = folderPath.split('/')[0]; // Get the root folder name
  
  // Create single FormData for all files
  const formData = new FormData();
  formData.append('db_name', dbName);

  // Add all CSV files to the same FormData
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
    const response = await fetch('api/upload-mongodb', {
      method: 'POST',
      body: formData,
    });

    if (!response.ok) {
      throw new Error('Failed to upload files');
    }

    const data = await response.json();
    addMessage(data.message || `Successfully uploaded ${csvCount} tables to database "${dbName}"`, 'bot-message');
  } catch (err) {
    addMessage(`Error: Could not upload tables to database "${dbName}".`, 'bot-message');
    console.error(err);
  }
});