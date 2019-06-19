const {of, from,iif,isObservable,EMPTY } = require("rxjs")
const {tap, pluck, map, toPromise, throwIfEmpty, catchError, filter, concatAll, toArray, mergeMap, switchMap} = require("rxjs/operators")
const log = require("debug")('loopback:connector:elasticsearch');

class Elasticsearch_Connector_Utils {

    constructor() {
        this.lbToEsMethodMap = new Map([
            ["create","index"],
            ["destroyAll","deleteByQuery"],
            ["replaceById","index"],
            ["replaceOrCreate","index"],
            ["findAll","search"],
            ["findById","search"],
            ["updateAttributes","index"]
        ])
    }

    /**
     *
     * @param lbName
     * @returns {*}
     */
    getEsMethodName(lbName){
        const esName = this.lbToEsMethodMap.get(lbName);
        if(!esName){
            log(`ESConnector::Utils::getEsMethodName: loopback method ${lbName} has no elasticsearch equivalent. Create one in Utils constructor`)
            return null
        }
        return esName
    }

    /**
     * Point free functionnal programing toolbox
     * @param filter
     * @param idName
     * @returns {*}
     */
     $_pf(filter,idName){
            return {
                getParamFromFilters$:this.fromFilters$(filter),
                $getPropFromWhereClause(param){
                    return  this.getParamFromFilters$("where")
                        .pipe(pluck(param))
                        .toPromise()
                }
            }
    }

    /**
     * Rerieves the id parameter from filters clause
     * @param filter
     * @param idName
     * @returns {Promise<*>}
     */
    async $getIdFromFilter(filter,idName){
         try{
             const id = await this.$_pf(filter,idName).$getPropFromWhereClause(idName)
             return id
         } catch(e){
             log(e)
             return null
         }
    }

    /**
     *
     * @param filter
     * @returns {Promise<*>}
     */
    async $getWhereFromFilter(filter){
        try{
            const where = await this.$_pf(filter).getParamFromFilters$("where").toPromise()
            return where
        } catch(e){
            return null
        }
    }

    /**
     *
     * @param model
     * @param filter
     * @param connector
     */
    $buildQuery(model, filter, connector){
        return new Promise(async(resolve, reject) => {
           // const where = await this.$getWhereFromFilter(filter)
            const idName = connector.idName(model)
            //modelName, idName, criteria, size, offset
            const query = this.buildFilter(model,idName, filter)
            resolve(query)
        })
    }


    /**
     * Will retrieve a first level parameter from loopback filters
     * @param filters
     * @returns {Function}
     */
    fromFilters$(filters){
        return function(param){
            return of(filters)
                .pipe(
                    pluck(param),
                    filter(x => !(x === undefined))
                    /*,throwIfEmpty(() => new Error(`${param} is not available in filters`))*/
                )
        }
    }

    /**
     * Will build es sorting part of query
     * @param model
     * @param order
     * @returns {Promise<Array>}
     */
    async $buildOrder(model,order) {

        if (typeof order === 'string')
            order = order.split(',');

        return of(order).pipe(
            concatAll(),
            map(x => {
                let sortType = "asc"
                let field = (x.match(/^\w+\s/)[0]).trim()
                let desc = x.match(/\s+D/)
                if(desc) sortType = "desc"
                return{[field]:sortType}
            }),
            catchError(e => {throw new Error("Invalid order parameter!")}),
            toArray()
        ).toPromise()

    }

    async buildFilter(modelName, idName, criteria, size, offset) {

        log('ESConnector.prototype.buildFilter', 'model', modelName, 'idName', idName,
            'criteria', JSON.stringify(criteria, null, 0));

        if (idName === undefined || idName === null) {
            throw new Error('idName not set!');
        }

        let filter = {}//this.addDefaults(modelName, 'buildFilter');
        filter.body = {};

        if (size) {
            filter.size = size;
        }
        if (offset) {
            filter.from = offset;
        }

        if (criteria) {

            // `criteria` is set by app-devs, therefore, it overrides any connector level arguments
            if (criteria.limit !== undefined && criteria.limit !== null) {
                filter.body.size = criteria.limit;
            }
            if (criteria.skip !== undefined && criteria.skip !== null) {
                filter.body.from = criteria.skip;
            }
            else if (criteria.offset !== undefined && criteria.offset !== null) { // use offset as an alias for skip
                filter.body.from = criteria.offset;
            }
            if (criteria.id) {

                filter.body.query = this.buildWhere(modelName, idName, criteria.id).query;
            }
            if (criteria.order) {
                log('ESConnector.prototype.buildFilter', 'will delegate sorting to buildOrder()');
                try {
                    filter.body.sort = await this.$buildOrder(modelName,criteria.order);
                } catch(e) {
                    throw e
                }
            }
           /* else { // TODO: expensive~ish and no clear guidelines so turn it off?
                //let idNames = this.idNames(model); // TODO: support for compound ids?
                let modelProperties = this._models[modelName].properties;
                if (idName === 'id' && modelProperties.id.generated) {
                    //filter.body.sort = ['_id']; // requires mapping to contain: '_id' : {'index' : 'not_analyzed','store' : true}
                    log('ESConnector.prototype.buildFilter', 'will sort on _uid by default when IDs are meant to be auto-generated by elasticsearch');
                    filter.body.sort = ['_uid'];
                } else {
                    log('ESConnector.prototype.buildFilter', 'will sort on loopback specified IDs');
                    filter.body.sort = [idName]; // default sort should be based on fields marked as id
                }
            }*/
            if (criteria.where) {
                filter.body.query = this.buildWhere(modelName, idName, criteria.where).query;
            }
            // TODO: Include filter
            else if (criteria.suggests) { // TODO: remove HACK!!!
                if (this.settings.apiVersion.indexOf('5') === 0) {
                    filter.body = {
                        suggest: criteria.suggests
                    }; // assume that the developer has provided ES compatible DSL
                }
                else if (
                    this.settings.apiVersion.indexOf('2') === 0 ||
                    this.settings.apiVersion.indexOf('1') === 0)
                {
                    filter.body = criteria.suggests; // assume that the developer has provided ES compatible DSL
                }
            }
            else if (criteria.native) {
                filter.body = criteria.native; // assume that the developer has provided ES compatible DSL
            }
            else if (Object.keys(criteria).length === 0) {
                filter.body = {
                    'query': {
                        'match_all': {}
                    }
                };
            }
        }

        log('ESConnector.prototype.buildFilter', 'constructed', JSON.stringify(filter, null, 0));
        return filter;
    }

    /**
     *
     * @param model
     * @param idName
     * @param where
     * @returns {{query: {bool: {must: Array, should: Array, must_not: Array}}}}
     */
    buildWhere(model, idName, where) {

        log('ESConnector::Utils::buildWhere', 'model', model, 'idName', idName, 'where', JSON.stringify(where, null, 0));

        let body = {
            query: {
                bool: {
                    must: [],
                    should: [],
                    must_not: []
                }
            }
        };
        this.buildNestedQueries(body, model, idName, where);
        if (body && body.query && body.query.bool && body.query.bool.must && body.query.bool.must.length === 0) {
            delete body.query.bool['must']; // jshint ignore:line
        }
        if (body && body.query && body.query.bool && body.query.bool.should && body.query.bool.should.length === 0) {
            delete body.query.bool['should']; // jshint ignore:line
        }
        if (body && body.query && body.query.bool && body.query.bool.must_not && body.query.bool.must_not.length === 0) { // jshint ignore:line
            delete body.query.bool['must_not']; // jshint ignore:line
        }
        if (body && body.query && body.query.bool && !Object.keys(body.query.bool).length) {
            delete body.query['bool'];// jshint ignore:line
        }
        if (body && body.query && !Object.keys(body.query).length) {
            body.query = {
                match_all: {}
            }
        }
        return body;
    };

    buildNestedQueries(body, model, idName, where) {
        /**
         * Return an empty match all object if no property is set in where filter
         * @example {where: {}}
         */
        if(!where){
            return
            //return {bool: {must: [{match_all:{}}]}};
        }

        if (Object.keys(where).length === 0) {
            body.match_all = {};
            log('ESConnector::Utils::buildNestedQueries', '\nbody', JSON.stringify(body, null, 0));
            return body;
        }
        let rootPath = body.query;
        this.buildDeepNestedQueries(true, idName, where, body, rootPath);
    }

    buildDeepNestedQueries(root, idName, where, body, path) {

        Object.keys(where).forEach(function (key) {
            let value = where[key];
            let cond = value;
            if (key === 'id' || key === idName) {
                key = idName;
            }

            if (key === 'and' && Array.isArray(value)) {
                let andPath;
                if (root) {
                    andPath = path.bool.must;
                }
                else {
                    let andObject = {bool: {must: []}};
                    andPath = andObject.bool.must;
                    path.push(andObject);
                }
                cond.map(function (c) {
                    log('ESConnector::Utils::buildDeepNestedQueries', 'mapped', 'body', JSON.stringify(body, null, 0));
                    this.buildDeepNestedQueries(false, idName, c, body, andPath);
                });
            }
            else if (key === 'or' && Array.isArray(value)) {
                let orPath;
                if (root) {
                    orPath = path.bool.should;
                }
                else {
                    let orObject = {bool: {should: []}};
                    orPath = orObject.bool.should;
                    path.push(orObject);
                }
                cond.map(function (c) {
                    log('ESConnector::Utils::buildDeepNestedQueries', 'mapped', 'body', JSON.stringify(body, null, 0));
                    this.buildDeepNestedQueries(false, idName, c, body, orPath);
                });
            }
            else {
                let spec = false;
                let options = null;
                if (cond && cond.constructor.name === 'Object') { // need to understand
                    options = cond.options;
                    spec = Object.keys(cond)[0];
                    cond = cond[spec];
                }
                log('ESConnector::Utils::buildNestedQueries',
                    'spec', spec, 'key', key, 'cond', JSON.stringify(cond,null,0), 'options', options);
                if (spec) {
                    if (spec === 'gte' || spec === 'gt' || spec === 'lte' || spec === 'lt') {
                        let rangeQuery = {range:{}};
                        let rangeQueryGuts = {};
                        rangeQueryGuts[spec] = cond;
                        rangeQuery.range[key] = rangeQueryGuts;
                        if(root){
                            path.bool.must.push(rangeQuery);
                        }
                        else {
                            path.push(rangeQuery);
                        }
                    }

                    /**
                     * Logic for loopback `between` filter of where
                     * @example {where: {size: {between: [0,7]}}}
                     */
                    if (spec === 'between') {
                        if (cond.length == 2 && (cond[0] <= cond[1])) {
                            let betweenArray = {range: {}};
                            betweenArray.range[key] = {
                                gte: cond[0],
                                lte: cond[1]
                            };
                            if(root){
                                path.bool.must.push(betweenArray);
                            }
                            else {
                                path.push(betweenArray);
                            }
                        }
                    }
                    /**
                     * Logic for loopback `inq`(include) filter of where
                     * @example {where: { property: { inq: [val1, val2, ...]}}}
                     */
                    if (spec === 'inq') {
                        let inArray = {terms: {}};
                        inArray.terms[key] = cond;
                        if (root) {
                            path.bool.must.push(inArray);
                        }
                        else {
                            path.push(inArray);
                        }
                        log('ESConnector.prototype.buildDeepNestedQueries',
                            'body', body,
                            'inArray', JSON.stringify(inArray, null, 0));
                    }

                    /**
                     * Logic for loopback `nin`(not include) filter of where
                     * @example {where: { property: { nin: [val1, val2, ...]}}}
                     */
                    if (spec === 'nin') {
                        let notInArray = { terms : {}};
                        notInArray.terms[key] = cond;
                        if(root){
                            path.bool.must_not.push(notInArray);
                        }
                        else {
                            path.push({bool: {must_not: [notInArray]}});
                        }
                    }

                    /**
                     * Logic for loopback `neq` (not equal) filter of where
                     * @example {where: {role: {neq: 'lead' }}}
                     */
                    if (spec === 'neq') {
                        /**
                         * First - filter the documents where the given property exists
                         * @type {{exists: {field: *}}}
                         */
                        // let missingFilter = {exists :{field : key}};
                        /**
                         * Second - find the document where value not equals the given value
                         * @type {{term: {}}}
                         */
                        let notEqual = { term : {}};
                        notEqual.term[key] = cond;
                        /**
                         * Apply the given filter in the main filter(body) and on given path
                         */
                        if (root) {
                            path.bool.must_not.push(notEqual);
                        }
                        else {
                            path.push({bool:{must_not: [notEqual]}});
                        }
                        // body.query.bool.must.push(missingFilter);
                    }
                    // TODO: near - For geolocations, return the closest points, sorted in order of distance.  Use with limit to return the n closest points.
                    // TODO: like, nlike
                    // TODO: ilike, inlike
                    // TODO: regex
                }
                else {
                    let nestedQuery = {match: {}};
                    // let nestedQuery = {query: { match: {}}};
                    nestedQuery.match[key] = value;
                    if (root) {
                        path.bool.must.push(nestedQuery)
                    }
                    else {
                        path.push(nestedQuery);
                    }
                    log('ESConnector.prototype.buildDeepNestedQueries',
                        'body', body,
                        'nestedQuery', JSON.stringify(nestedQuery, null, 0));
                }
            }
        });
    };



    getSource(x){
        if(x.hasOwnProperty("hits")) return x.hits
        else return x
    }

    /**
     * will parse elastic 's result for user expected data
     * @param qr - elasticsearch query result
     */
    mapResult$(qr){

        const getHits = x => {
            if(x.hasOwnProperty("hits")){
                x = x.hits;
                if(x.hasOwnProperty("hits")){
                    x = x.hits;
                }
                return x
            } else return x
        }

        const getSource = x => {
            if(x.hasOwnProperty("_source")) return x._source
            else return x
        }

        return of(qr)
            .pipe(
                pluck("body"),
                map(getHits),
               // tap(console.log),
                switchMap(
                    x => {
                        return iif(()=> x instanceof Array,
                            of(x).pipe(
                                concatAll()
                            ),
                            of(x)
                        )
                    }
                ),
                map(getSource),
                toArray()
            )
    }

    /**
     * Retrieves the model index name ( needs to be set in the model settings)
     * @param model
     * @param connector
     * @returns {null}
     */
    getModelIndex(model,connector){
        let modelSettings = connector.getModelDefinition(model).settings;
        if(modelSettings.hasOwnProperty("index") && modelSettings.index)
            return modelSettings.index
        else throw new Error("You need to set the es index name in the model's settings")

    }

    /**
     * Parse status code to define state of request
     * @param statusCode
     * @returns {boolean}
     */
    isHTTPSuccess(statusCode){
        let code = 500;
        if(statusCode){
            if(Number.isInteger(statusCode)) code = (statusCode).toString()
            else code = statusCode
        }

        if(code.match(/^2/)) return true
        else return false
    }

    formatErrorMessage(err){
        try{
            let msg = JSON.stringify(err.meta.body.error);
            if(msg) return `${err.message}: ${msg}`
        } catch(e){
            return `${err.message}`
        }

    }

}

module.exports = new Elasticsearch_Connector_Utils();