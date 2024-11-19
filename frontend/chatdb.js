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
    fetch('/query', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ query })
    })
    .then(response => response.json())
    .then(data => addMessage(data.response, 'bot-message'))
    .catch(err => addMessage('Error: Could not fetch response.', 'bot-message'));
});

function addMessage(text, className) {
    const msg = document.createElement('div');
    msg.textContent = text;
    msg.classList.add('message', className);
    messages.appendChild(msg);
    messages.scrollTop = messages.scrollHeight;
}
