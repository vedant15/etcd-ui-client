const electron = require('electron');
const {app, BrowserWindow, Menu, ipcMain, dialog} = electron;
const etcd = require('./lib');

function processConnectionRequest (err, options) {
  console.log(err);
  console.log(options);
}

app.on('ready', function () {
  let win = new BrowserWindow({width: 1024, height: 1024, webPreferences: {
      nodeIntegration: true
    }});

  win.loadURL(`file://${__dirname}/index.html`);

  //win.webContents.openDevTools();

  var template = [{
    label: "Application",
    submenu: [
      {label: "About Application", selector: "orderFrontStandardAboutPanel:"},
      {type: "separator"},
      {
        label: "Quit", accelerator: "Command+Q", click: function () {
        app.quit();
      }
      }
    ]
  }, {
    label: "Edit",
    submenu: [
      {label: "Cut", accelerator: "CmdOrCtrl+X", selector: "cut:"},
      {label: "Copy", accelerator: "CmdOrCtrl+C", selector: "copy:"},
      {label: "Paste", accelerator: "CmdOrCtrl+V", selector: "paste:"},
      {label: "Select All", accelerator: "CmdOrCtrl+A", selector: "selectAll:"}
    ]
  }
  ];

  Menu.setApplicationMenu(Menu.buildFromTemplate(template));
});

ipcMain.on('createConnection', function (event, connectionDetails) {
  etcd.init(connectionDetails, function (err) {
    event.sender.send('connectionResponse', err);
  });
});


ipcMain.on('getKey', function (event, params) {
  etcd.performEtcdOperation(etcd.SUPPORTED_OPERATIONS.GET, params, function (err, response) {
    const finalResponse = {
      err: err,
      result: response
    };
    event.sender.send('getKeyResponse', finalResponse);
  });
});

ipcMain.on('createKey', function (event, params) {
  etcd.performEtcdOperation(etcd.SUPPORTED_OPERATIONS.CREATE, params, function (err, response) {
    const finalResponse = {
      err: err,
      result: response
    };
    event.sender.send('createKeyResponse', finalResponse);
  });
});

ipcMain.on('deleteKey', function (event, params) {
  etcd.performEtcdOperation(etcd.SUPPORTED_OPERATIONS.DELETE, params, function (err, response) {
    const finalResponse = {
      err: err,
      result: response
    };
    event.sender.send('deleteKeyResponse', finalResponse);
  });
});

ipcMain.on('updateKey', function (event, params) {
  etcd.performEtcdOperation(etcd.SUPPORTED_OPERATIONS.UPDATE, params, function (err, response) {
    const finalResponse = {
      err: err,
      result: response
    };
    event.sender.send('updateKeyResponse', finalResponse);
  });
});

ipcMain.on('openFileSelector', function (event, params) {
  dialog.showOpenDialog({ properties: ['openFile'] }, function (filePaths) {
    event.sender.send('openFileSelectorResponse', filePaths ? filePaths[0] : null);
  });
});