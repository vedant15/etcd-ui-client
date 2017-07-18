const electron = require('electron');
const {app, BrowserWindow, Menu, ipcMain} = electron;

app.on('ready', function () {
    let win = new BrowserWindow({width : 1024, height : 1024});
    win.loadURL(`file://${__dirname}/index.html`);

    win.webContents.openDevTools();
});

ipcMain.on('invokeConnection', function(event, connectionDetails){
    event.sender.send('connectionResponse', connectionDetails);
});