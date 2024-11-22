// script.js
const sendBtn = document.getElementById('send-btn');
const userInput = document.getElementById('user-input');
const messages = document.getElementById('messages');

sendBtn.addEventListener('click', () => {
    const query = userInput.value.trim();
    if (!query) return;

    addMessage(query, 'user-message');
    userInput.value = '';

    // Call Flask backend
    fetch('/api/query', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ query })
    })
    .then(response => response.json())
    .then(data => {
        addMessage(data.response, 'bot-message');
        displayTable(data.results);
    })
    .catch(err => addMessage('Error: Could not fetch response.', 'bot-message'));

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
  uploadFileInput.click(); // Trigger the hidden file input
});

uploadFileInput.addEventListener('change', () => {
  const file = uploadFileInput.files[0];
  if (!file) return;

  // Validate the file type
  if (!file.name.endsWith('.csv')) {
    addMessage('Error: Please upload a valid CSV file.', 'bot-message');
    return;
  }

  const formData = new FormData();
  formData.append('file', file);

  // Call Flask backend to upload the file
  fetch('api/upload-mysql', {
    method: 'POST',
    body: formData,
  })
    .then(response => {
      if (!response.ok) {
        throw new Error('Failed to upload file.');
      }
      return response.json();
    })
    .then(data => {
      addMessage(data.message || 'File uploaded successfully!', 'bot-message');
    })
    .catch(err => {
      addMessage('Error: Could not upload file.', 'bot-message');
      console.error(err);
    });
});