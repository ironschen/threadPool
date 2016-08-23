/**
 * Created by ironschen on 2016/6/13.
 */

var Q = require('q');
var fs = require('fs');

var ThreadPool = function (init_opt, Crawler) {
    this._Crawler = Crawler;
    this._info = {};
    this._info.start_time = null;
    if(typeof(init_opt)=="string") {
        this._info = JSON.parse(fs.readFileSync(init_opt));
    } else {
        this._info.list = init_opt.list;
        this._info.index = init_opt.index ? parseInt(init_opt.index, 10) : 0;
        this._info.thread = init_opt.thread;
        this._info.thread_index = {};
        for(var i=0; i<this._info.thread; i++) {
            this._info.thread_index[i] = null;   
        }
    }
};

ThreadPool.prototype.saveInfo = function () {
    var d = new Date();
    fs.writeFileSync("save_tp_"+Math.ceil(d.getTime()/1000)+".txt", JSON.stringify(this));
};

ThreadPool.prototype.run = function () {
    var df = Q.defer();
    var current = this;
    current._info.start_time = new Date();
    console.log("start time: "+current._info.start_time.toString());

    var it = setInterval(doLoop, 10);

    return df.promise;

    function doLoop() {
        var threadEmpty = checkThread();
        if( current._info.index>=current._info.list.length
            && threadEmpty.length==current._info.thread) {
            console.log("all crawler finish");
            var end = new Date();
            console.log("start time: "+current._info.start_time.toString());
            console.log("end time: "+end.toString());
            clearInterval(it);
            df.resolve(current);
        }

        if(threadEmpty.length>0 && current._info.index<current._info.list.length) {
            //console.log("set thread "+threadEmpty[0]);
            setThread(threadEmpty[0]);
        }
    }

    function setThread(tid) {
        var date = new Date();
        console.log(date.toLocaleString()+", set thread "+tid+" of "+current._info.thread
            +" task, total: "+current._info.index+"/"+current._info.list.length);

        var cOpt = current._info.list[current._info.index];
        current._info.thread_index[tid] = current._info.index;
        current._info.index++;

        setCrawler(cOpt);

        function setCrawler(crawlerOpt) {
            var cp = new current._Crawler(crawlerOpt);

            cp.doProcess().then(function (ptr) {
                current._info.thread_index[tid] = null;
            }).catch(function (err) {
                var date = new Date();
                console.error(date.toLocaleString()+", thread: "+tid+", error: "+err);
                setCrawler(crawlerOpt);
            });
        }
    }

    function checkThread() {
        var list = [];
        for(var i=0; i<current._info.thread; i++) {
            if(current._info.thread_index[i]==null) {
                list.push(i);
            }
        }

        return list;
    }
};


module.exports = ThreadPool;
