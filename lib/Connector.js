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

    /**
     * Connect to elasticsearch
     * @param {function} callback
     */
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

    /**
     * let loopback know wheter elasticsearch is responding or not
     * @param {function} cb
     */
    async ping(cb){
        try {
            const { body, statusCode, headers, warnings }  = await this.db.ping();
            if(Utils.isHTTPSuccess(statusCode)) cb(null,true)
            else cb(null)
        } catch(e){
            log(e)
            cb(null)
        }
    }


    /**
     * Find matching model instances by the filter
     * @param {String} model The model name
     * @param {Object} filter The filter
     * @param {Function} [done] The callback function
     * @returns {Promise<object>}
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
     * Implement loopback's find method
     * @param {String} model
     * @param {String} id
     * @param {Object} options
     * @param {Function} callback
     * @returns {Function}
     */
    async find(model, id, options, callback){

        return this.findById(id,model)
    }

    /**
     * Find a model instance by id
     * @param {String} id
     * @param {String} model
     * @returns {Promise<*>}
     */
    async findById(id, model){

        let idName = this.idName(model)
        let data = {where:{[idName]:id}}
        const query = await Utils.$buildQuery(model, data, this)

        try {
            let result = await this.executeQuery(query,model,"findById")
            return result
        } catch(e){
            e.message = Utils.formatErrorMessage(e)
            log(e)
            return null
        }

    }

    /**
     * Find all instances of a model
     * @param {String} model
     * @param {Object} filter
     * @returns {Promise<*>}
     */
    async findAll(model,filter) {
        const query = await Utils.$buildQuery(model, filter, this)
        const result = await this.executeQuery(query,model,"findAll")
        return result
    }

    /**
     * Creates a model instance
     * @param model
     * @param data
     * @param cb
     * @returns {Promise<void>}
     */
    async create(model, data, cb){
        try {
            const result = await this.executeQuery(data,model,"create")
            cb(null,result)
        } catch(e){
            e.message = Utils.formatErrorMessage(e)
            log(e)
            cb(e)
        }
    }

    /**
     * Delete a model instance
     * @param model
     * @param filter
     * @param cb
     * @returns {Promise<void>}
     */
    async destroyAll(model, filter, cb){
        try {
            if(!filter.hasOwnProperty("where")){
                filter = {where:filter}
            }
            //const idName = this.idName(model)
            //const id = await Utils.$getIdFromFilter(filter,idName)
            const query   = await Utils.$buildQuery(model, filter, this)
            const result  = await this.executeQuery(query, model, "destroyAll")
            cb(null,result)
        } catch(e){
            e.message = Utils.formatErrorMessage(e)
            log(e)
            cb(e)
        }

    }

    /**
     *
     * @param model
     * @param id
     * @param data
     * @param options
     * @param cb
     * @returns {Promise<void>}
     */
    async replaceById(model, id, data, options, cb){
       try {
           let result  = await this.executeQuery(data, model, "replaceById")
           cb(null,result)
       } catch(e){
           log(e)
           e.message = Utils.formatErrorMessage(e)
           cb(e)
       }

    }

    /**
     *
     * @param model
     * @param data
     * @param options
     * @param cb
     * @returns {Promise<void>}
     */
    async replaceOrCreate(model, data, options, cb){

        try {
            let result  = await this.executeQuery(data, model, "replaceOrCreate")
            cb(null,result)
        } catch(e){
            e.message = Utils.formatErrorMessage(e)
            cb(e)
        }
    }

    /**
     *
     * @param model
     * @param data
     * @param options
     * @param cb
     * @returns {Promise<void>}
     */
    async patchOrCreate(model, data, options, cb){}

    /**
     *
     * @param model
     * @param id
     * @param data
     * @param options
     * @param cb
     * @returns {Promise<void>}
     */
    async updateAttributes(model, id, data, options, cb){
        try {
            let entity = await this.findById(id,model)
            if(entity[0]) entity = entity[0]
            let merged = Object.assign({},entity,data)
            try {
                let result  = await this.executeQuery(merged, model, "updateAttributes")
                cb(null,result)
            } catch(e){
                e.message = Utils.formatErrorMessage(e)
                throw e
            }
        } catch(e){
            cb(e)
        }
    }

    /**
     *
     * @param query
     * @param model
     * @param method
     * @returns {Promise<*>}
     */
    async executeQuery(query, model, method){

        method = Utils.getEsMethodName(method)
        if(!method) return []

        let index = Utils.getModelIndex(model,this)
        let body = (query.hasOwnProperty("body") ? query.body : query)

        const qb = {
            index,
            body
        };

        if(method.indexOf(["replaceOrCreate","update","patchOrCreate"]) ){
            let idName  = this.idName(model)
            qb.id = query[idName]

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


