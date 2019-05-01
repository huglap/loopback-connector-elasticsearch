const Connector = require("loopback-connector").Connector;
const log       = require("debug")('loopback:connector:elasticsearch');
const {Client}  = require("@elastic/elasticsearch")
const assert    = require("assert");

class Elasticsearch_Connector extends Connector {

    constructor(settings, dataSource) {
        super(settings);
        this._settings = dataSource.settings;
        assert(dataSource.settings.clientSettings,"clientSettings are not set");
        this.clientSettings = dataSource.settings.clientSettings;

    }

    get Client(){
        if(!this._client) throw new Error("elasticsearch client is unavailable at this stage. You must call the connect method.")
        return  this._client;
    }

    connect(){
        return new Promise(resolve => {
            if(!this._client) this._client = new Client(this.clientSettings)
            resolve(this.Client);
        })
    }

    ping(){
        return this._client.ping();
    }

    create(model, data){
        console.log(data)
    }

    update(){}

    async find(modelName, id, cb){

        try {
            const { body, statusCode, headers, warnings } = this._client.get({
                id,
                index:modelName
            })
            cb(body)
        } catch(e){

        }
    }
}




module.exports.initialize = function (dataSource, callback) {

    const settings = dataSource.settings || {};
    let instance = new Elasticsearch_Connector(settings, dataSource);
    dataSource.connector = instance;

    if (callback) {
        dataSource.connector.connect(callback);
    }
};
