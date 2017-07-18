const {ipcRenderer} = require('electron');

function initalizeConnection () {

    let connectionDetails = {
        connectionString : document.getElementById('connection_string').value
    };

    ipcRenderer.once('connectionResponse', function(event, response){
        console.log('got response: ' + JSON.stringify(response));
    });
    ipcRenderer.send('invokeConnection', connectionDetails);
}
