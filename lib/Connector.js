const Connector = require("loopback-connector").Connector;
const log       = require("debug")('loopback:connector:elasticsearch');
const {Client}  = require("@elastic/elasticsearch")
const assert    = require("assert");
const {of, from}      = require("rxjs")
const {tap, pluck, map, toPromise, toArray, throwIfEmpty, catchError, filter, concatMap, mergeMap,concatAll}  = require("rxjs/operators")
const Utils = require("./Utils");

class Elasticsearch_Connector extends Connector {

    constructor(settings, dataSource) {
        super(settings);
        this._dataSource = dataSource;
        this._settings = dataSource.settings;
        assert(dataSource.settings.clientSettings,"clientSettings are not set");
        this.clientSettings = dataSource.settings.clientSettings;
    }

    get Client(){
        if(!this.db) throw new Error("elasticsearch client is unavailable at this stage. You must call the connect method.")
        return  this.db;
    }

    connect(callback){

        if (this.db) {
            process.nextTick(function() {
                callback && callback(null, this.db);
            });
        } else if (this._dataSource.connecting) {
            this._dataSource.once('connected', function() {
                process.nextTick(function() {
                    callback && callback(null, this.db);
                });
            });
        } else {
            this.db = new Client(this.clientSettings)
            callback && callback(null, this.db);
        }

    }

    async ping(cb){
        try {
            const { body, statusCode, headers, warnings }  = await this.db.ping();
            if(Utils.isHTTPSuccess(statusCode)) cb(null,1)
            else cb(null)
        } catch(e){
            log(e)
            cb(null)
        }

    }

    update(){}

    /**
     * Find matching model instances by the filter
     * @param {String} model The model name
     * @param {Object} filter The filter
     * @param {Function} [done] The callback function
     * @returns {Promise<void>}
     */
    async all(model, filter, done){
        let result = []
        let id = null
        let idName = this.idName(model)

        //console.log(Utils.buildFilter(model,idName,filter))

        try {
            id = await Utils.$getIdFromFilter(filter,idName)
            if(id){
                result = await this.findById(id,model)
                return done(null,result)
            }
            const findAll = await this.findAll(model, filter)
            done(null, findAll)
        } catch(e){
            log(e)
            done(e)
        }
    }


    /**
     *
     * @param modelName
     * @param id
     * @param cb
     * @returns {Promise<void>}
     */
    async find(model, id, options, callback){

        let filter = {};
        if(id && !filter.hasOwnProperty("where")){
            filter.where = {id};
        } else if(id && filter.hasOwnProperty("where"))
            filter.where.id = id;

        return this.all(model,filter, callback)
    }
    /*
    async save(model, data, done) {

        if (this.debug) {
            log('ESConnector.save ', 'model', model, 'data', data)
        }

        var idName = this.idName(model)
        var defaults = this.addDefaults(model, 'save')
        var id = this.getDocumentId(data[idName])

        if (id === undefined || id === null) {
            return done('Document id not setted!', null)
        }

        this.db.update(_.defaults({
            id: id,
            body: {
                doc: data,
                'doc_as_upsert': false
            }
        }, defaults))
            .then(function (response) {
                    done(null, response);
                }, function (err) {
                    if (err) {
                        return done(err, null);
                    }
                }
            );
    };
    */


    async findById(id, model){

        const index = Utils.getModelIndex(model,this);

        const { body, statusCode, headers, warnings } = await this.db.get({
            id,
            index,
            type:"_all"
        })

        if(Utils.isHTTPSuccess(statusCode) && body.hasOwnProperty("_source")) return [body._source]
        else return []

    }

    async findAll(model,filter) {

        const query = await Utils.$buildQuery(model, filter, this)
        const result = await this.executeQuery(query,model,"findAll")
        return result
    }

    async create(model, data, cb){
//console.log(333,data)
        try {
            const result = await this.executeQuery(data,model,"create")
            cb(null,result)
        } catch(e){
            log(e)
            cb(null)
        }

    }

    async replaceById(model, id, data, options, cb){
       try {
           let result  = await this.executeQuery(data, model, "replaceById")
           cb(null,result)
       } catch(e){
           cb(e)
       }

    }

    async executeQuery(query, model, method){

        method = Utils.getEsMethodName(method)
        if(!method) return []

        let index = Utils.getModelIndex(model,this)
        let body = (query.hasOwnProperty("body") ? query.body : query)

        const qb = {
            index,
            body
        };

        if(method === "update"){
            qb.id = query.id
        }

        const sResults = await from(this.db[method](qb))
            .pipe(mergeMap(Utils.mapResult$))
            .toPromise()

        return sResults
    }

}


exports.initialize = (dataSource, callback) => {

    const settings = dataSource.settings || {};
    let instance = new Elasticsearch_Connector(settings, dataSource);
    dataSource.connector = instance;

    if (callback) {
        dataSource.connector.connect(callback);
    }
};


