const {ipcRenderer} = require('electron');
const util = require('util');
const uuid = require('uuid');
const Datastore = require('nedb');


const db = new Datastore({ filename: __dirname + '/saved_connections.store', autoload: true });
const TABLE_KEY_PREFIX = 'e9e6285c';
const HIDDEN_KEY_ID_SELECTOR = '#selected_key';

let valueRegistry = {};
let tableIdRegistry = {};
let savedConnections = null;

db.find({}, function (err, docs) {
  savedConnections = docs;
  docs.forEach(function (doc) {
    $('#saved_connections_selector').append($('<option>', {
      value: doc._id,
      text: doc._id
    }));
  });
});

ipcRenderer.on('createKeyResponse', function(event, response){
    if (response.err) {
        alert('Request to create Key failed: ' + JSON.stringify(response.err));
    } else {
        alert('Key was added');
    }
});

ipcRenderer.on('updateKeyResponse', function(event, response){
    if (response.err) {
        alert('Request to updated Key failed: ' + JSON.stringify(response.err));
    } else {
        alert('Key was updated');
    }
});

ipcRenderer.on('getKeyResponse', function(event, response){
    if (response.err) {
        alert('Request to get Key Failed: ' + JSON.stringify(response.err));
    } else {
        valueRegistry = {};
        tableIdRegistry = {};
        $('#key_details_data').html('<pre></pre>');
        $('#selected_key').val('');
        $('#keys_name_table').html(generateKeysTable(response.result));
    }
});

function initalizeConnection () {
    const connectionDetails = {};

    $('.connection_form_input').each(function () {
        connectionDetails[$(this).attr('name')] = $(this).val();
    });

    $('.connection_form_input_check_box').each(function () {
        connectionDetails[$(this).attr('name')] = $(this).is(':checked');
    });

    connectionDetails._id = connectionDetails.connectionName ? connectionDetails.connectionName.trim() : uuid.v4();

    if (connectionDetails.saveDetails) {
      db.update({_id : connectionDetails._id}, connectionDetails, { upsert: true });
    }

    ipcRenderer.once('connectionResponse', function(event, response){
        if (response) {
            alert('Connection failed: ' + JSON.stringify(response));
        } else {
            $('#connection_form').hide();
            $('.main-app').css('z-index', 1);
            $('#footer_title').html('Connected to: ' + (connectionDetails.connectionString || '127.0.0.1:2379'));
        }
    });
    ipcRenderer.send('createConnection', connectionDetails);
}

function getKeyTableId (key) {
    return tableIdRegistry[key];
}

function generateKeysTable (kvs) {
    let responseHtml = '<tbody>';

    for(let i = 0; i < kvs.length; i++) {
        valueRegistry[kvs[i].key] = kvs[i].value;
        tableIdRegistry[kvs[i].key] = uuid.v4();
        responseHtml += util.format('<tr id="%s" onclick="switchSelectedKey(\'%s\')"><td>%s</td></tr>', getKeyTableId(kvs[i].key), kvs[i].key, kvs[i].key);
    }
    return responseHtml + '</tbody>';
}

function getKeys () {
    const params = {
        key : $('#search_bar_input').val()
    };
    ipcRenderer.send('getKey', params);
}

function switchSelectedKey(key) {
    const actualId = '#' + getKeyTableId(key);
    const value = valueRegistry[key];

    if ($(HIDDEN_KEY_ID_SELECTOR).val()) {
        const oldSelectedId = '#' + getKeyTableId($(HIDDEN_KEY_ID_SELECTOR).val());
        $(oldSelectedId).removeClass('selected_key');
    }

    $(HIDDEN_KEY_ID_SELECTOR).val(key);
    $(actualId).addClass('selected_key');
    $('#key_details_data').html('<pre style="user-select: text">' + value + '</pre>');
}

function showCreateKeyModalForm() {
    $('#create_key_form_container').dialog();
}

function createKey() {
    const keyDetails = {};
    $('.create_key_form_input').each(function () {
        keyDetails[$(this).attr('name')] = $(this).val().trim();
        $(this).val('');
    });

    $('#create_key_form_container').dialog('destroy');

    if (window.confirm('Do you want to create: ' + JSON.stringify(keyDetails)) == true) {
        ipcRenderer.send('createKey', keyDetails);
    } else {
        return false;
    }
}

function showUpdateKeyModalForm() {
    $('#update_key_form_container').dialog();
}

function updateKey() {
    const keyDetails = {
        key : $(HIDDEN_KEY_ID_SELECTOR).val(),
        value : $('#update_key_form_key_value').val()
    };

    $('#update_key_form_container').dialog('destroy');

    if (window.confirm('Do you want to update: ' + JSON.stringify(keyDetails)) == true) {
        ipcRenderer.send('updateKey', keyDetails);
    }
}

function deleteKey () {
    const requestedKey = $(HIDDEN_KEY_ID_SELECTOR).val();
    if (window.confirm('Do you want to delete: ' + requestedKey) == true) {
        ipcRenderer.once('deleteKeyResponse', function(event, response){
            if (response.err) {
                alert('Request to delete Key failed: ' + JSON.stringify(response.err));
            } else {
                alert('Key was deleted');
                $(HIDDEN_KEY_ID_SELECTOR).val();
                $('#' + getKeyTableId(requestedKey)).remove();
            }
        });
        ipcRenderer.send('deleteKey', {key : requestedKey});
    }
}

function loadSavedConnection () {
  const selectedConnection = $('#saved_connections_selector').val();
  savedConnections.forEach(function (savedDetails) {
    if (savedDetails._id === selectedConnection) {
      $('.connection_form_input').each(function () {
        $(this).val(savedDetails[$(this).attr('name')]);
      });
    }
  });
}