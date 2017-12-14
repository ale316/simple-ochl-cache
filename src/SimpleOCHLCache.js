const moment = require('moment')
const _ = require('lodash')

class SimpleOCHLCache {
    constructor(opts = {}) {
        // the time to live of the candles
        this.ttl = opts.ttl // milliseconds
        // the size of each candle
        this.size = opts.size // milliseconds
        // the minimum number of candles to keep in store
        this.minSize = _.isNil(opts.minSize) ? 2 : opts.minSize

        // PRIVATE methods
        this._defaultCandle = this._defaultCandle.bind(this)
        this._prune = this._prune.bind(this)
        // PUBLIC methods
        this.add = this.add.bind(this)
        this.get = this.get.bind(this)

        this.data = {}
    }

    _defaultCandle(t) {
        return {
            t: Math.floor(t.d / this.size) * this.size,
            ot: t.d, // open time
            ct: t.d, // close time
            o: t.r, // open
            c: t.r, // close
            h: t.r, // high
            l: t.r, // low
            v: t.q // volume
        }
    }

    updateCandle(candle, trades) {
        if (_.isEmpty(candle)) {
            candle = this._defaultCandle(trades[0])
            trades = trades.slice(1)
        }

        return trades.reduce((c, t) => {
            c.o = t.d < c.ot ? t.r : c.o; // open time
            c.ot = t.d < c.ot ? t.d : c.ot; // close time
            c.c = t.d > c.ot ? t.r : c.o; // open
            c.ct = t.d > c.ct ? t.d : c.ct; // close
            c.h = t.r > c.h ? t.r : c.h; // high
            c.l = t.r < c.l ? t.r : c.l; // low
            c.v += t.q; // volume
            return c
        }, candle)
    }

    _prune(data) {
        if (_.isEmpty(data)) return []

        const ttl = moment().utcOffset(0).valueOf() - this.ttl

        let pruned = _.filter(data, (d) => d.t >= ttl)
        if (!pruned || pruned.length < this.minSize)
            pruned = _.takeRight(data, this.minSize)

        return pruned
    }

    /*
        @input items ~ Item { timestamp: Date, market_name: String, quantity: Float, rate: Float } || [Item]
    */
    add(key, items) {
        if (!_.isArray(items)) items = [items]
        if (_.isNil(this.data[key])) this.data[key] = []
        // group by the candle the trades belong to
        let data = this.data
        const grouped = _.groupBy(items, (i) => Math.floor(i.d / this.size) * this.size)
        _.each(grouped, (trades, ts) => {
            // find index at which to insert
            ts = parseInt(ts)
            const idx = _.sortedIndexBy(data[key], { t: ts }, 't')
            if (_.get(data, `["${key}"][${idx}].t`) !== ts)
                data[key].splice(idx, 0, {})

            data[key][idx] = this.updateCandle(data[key][idx], trades)
        })
        data[key] = this._prune(data[key])
    }

    get(key) {
        let data = this.data[key] || []
        this.data[key] = this._prune(data)
        let result = this.data[key]

        result = _.map(result, (v) => {
            return {
                t: v.t, // time
                o: v.o, // open
                c: v.c, // close
                h: v.h, // high
                l: v.l, // low
                v: v.v // volume
            }
        })

        return result
    }
}

export class SimpleOCHLCache;