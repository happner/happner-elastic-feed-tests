describe('happner-elastic-feed-performance-tests', function () {

  this.timeout(5000);

  var expect = require('expect.js');

  var Service = require('happner-elastic-feed');

  var async = require('async');

  var uuid = require('uuid');

  var N = 100;

  var T = 10000;

  var UPDATE_MOD = 10;

  it('does ' + N + ' or more jobs in ' + T + ' milliseconds', function (done) {

    this.timeout(T + 10000);

    var queueService = new Service();

    var subscriberService = new Service();

    var emitterService = new Service();

    var feedRandomName = uuid.v4();

    var queueConfig = {
      queue: {
        jobTypes: {
          "emitter": {concurrency: 10}
        }
      }
    };

    var subscriberConfig = {};

    var subscriberWorkerConfig = {
      name: 'happner-emitter-worker',
      queue: {username: '_ADMIN', password: 'happn', port: 55000, jobTypes: ["feed"]},
      data: {
        port: 55001
      }
    };

    var emitterWorkerConfig = {
      name: 'happner-emitter-worker',
      queue: {username: '_ADMIN', password: 'happn', port: 55000, jobTypes: ["emitter"]},
      data: {
        port: 55002
      }
    };

    var feedData = {
      action: 'create',
      name: 'subscriber test ' + feedRandomName,
      datapaths: [
        '/device/1/*',
        '/device/2/*',
        '/device/3/*'
      ],
      state: 2
    };

    var feedId;

    var setJobCount = 0;

    var completedJobCount = 0;

    var startedJobCount = 0;

    var completedAlready = false;

    var started, completed;

    started = Date.now();

    var finish = function (e) {

      if (!completedAlready) {

        completedAlready = true;

        emitterService.stop()
          .then(function () {
            return queueService.stop();
          })
          .then(function () {
            return subscriberService.stop();
          })
          .then(function () {
            done(e);
          })
          .catch(function (stopError) {

            console.warn('failed stopping services:::', stopError.toString());

            done(e);
          });
      }
    };

    queueService
      .queue(queueConfig)
      .then(function () {
        return subscriberService.worker(subscriberWorkerConfig);
      })
      .then(function () {
        return emitterService.worker(emitterWorkerConfig);
      })
      .then(function () {
        return emitterService.emitter(emitterWorkerConfig);
      })
      .then(function () {
        return subscriberService.subscriber(subscriberConfig);
      })
      .then(function () {

        return new Promise(function (resolve, reject) {

          emitterService.__mesh.event.emitter.on('handle-job', function (job) {

            startedJobCount++;

            if (startedJobCount % UPDATE_MOD == 0) console.log('started:' + startedJobCount.toString() + ' out of ' + N);
          });

          emitterService.__mesh.event.emitter.on('handle-job-failed', function (error) {

            finish(new Error(error.message));

          }, function (e) {

            if (e) return reject(e);

            emitterService.__mesh.event.emitter.on('handle-job-ok', function (results) {

              completedJobCount++;

              if (completedJobCount % UPDATE_MOD == 0) console.log('completed:' + completedJobCount.toString() + ' out of ' + N);

              if (completedJobCount >= N) {

                if (completedAlready) return;

                completed = Date.now();

                var completedIn = completed - started;

                console.log('completed in ' + completedIn + ' milliseconds.');

                if (completedIn >= T + 3000) return finish(new Error('not completed within the specified timeframe of ' + T + ' milliseconds'));

                return finish();
              }
            }, function (e) {

              if (e) return reject(e);

              resolve();
            });

          });
        });
      })
      .then(function () {

        return subscriberService.__mesh.exchange.feed.upsert(feedData);
      })
      .then(function (feed) {

        feedId = feed.id;

        var subscriberMesh = subscriberService.__mesh._mesh;

        started = Date.now();

        async.times(N, function (time, timeCB) {

          var random = Math.floor(Math.random() * 100).toString();

          subscriberMesh.data.set('/device/1/' + random, {test: random}, function (e) {

            if (e) return timeCB(e);

            setJobCount++;

            if (setJobCount % UPDATE_MOD == 0) console.log('set:' + setJobCount.toString() + ' out of ' + N);

            timeCB();

          }, finish);
        });
        //go back up to the second step worker listens on emitter
      })
      .catch(finish);

  });

  it('does ' + N + ' or more jobs in ' + T + ' milliseconds with data caching switched on (needs redis)', function (done) {

    this.timeout(T + 10000);

    var queueService = new Service({dataCache: true});

    var subscriberService = new Service({dataCache: true});

    var emitterService = new Service({dataCache: true});

    var feedRandomName = uuid.v4();

    var queueConfig = {
      queue: {
        jobTypes: {
          "emitter": {concurrency: 10}
        }
      }
    };

    var subscriberConfig = {};

    var subscriberWorkerConfig = {
      name: 'happner-emitter-worker',
      queue: {username: '_ADMIN', password: 'happn', port: 55000, jobTypes: ["feed"]},
      data: {
        port: 55001
      }
    };

    var emitterWorkerConfig = {
      name: 'happner-emitter-worker',
      queue: {username: '_ADMIN', password: 'happn', port: 55000, jobTypes: ["emitter"]},
      data: {
        port: 55002
      }
    };

    var feedData = {
      action: 'create',
      name: 'subscriber test ' + feedRandomName,
      datapaths: [
        '/device/1/*',
        '/device/2/*',
        '/device/3/*'
      ],
      state: 2
    };

    var feedId;

    var setJobCount = 0;

    var completedJobCount = 0;

    var startedJobCount = 0;

    var completedAlready = false;

    var started, completed;

    var finish = function (e) {

      if (!completedAlready) {

        completedAlready = true;

        emitterService.stop()
          .then(function () {
            return queueService.stop();
          })
          .then(function () {
            return subscriberService.stop();
          })
          .then(function () {
            done(e);
          })
          .catch(function (stopError) {

            console.warn('failed stopping services:::', stopError.toString());

            done(e);
          });
      }
    };

    started = Date.now();

    queueService
      .queue(queueConfig)
      .then(function () {
        return subscriberService.worker(subscriberWorkerConfig);
      })
      .then(function () {
        return emitterService.worker(emitterWorkerConfig);
      })
      .then(function () {
        return emitterService.emitter(emitterWorkerConfig);
      })
      .then(function () {
        return subscriberService.subscriber(subscriberConfig);
      })
      .then(function () {

        return new Promise(function (resolve, reject) {

          emitterService.__mesh.event.emitter.on('handle-job', function (job) {

            startedJobCount++;

            if (startedJobCount % UPDATE_MOD == 0) console.log('started:' + startedJobCount.toString() + ' out of ' + N);
          });

          emitterService.__mesh.event.emitter.on('handle-job-failed', function (error) {

            finish(new Error(error.message));

          }, function (e) {

            if (e) return reject(e);

            emitterService.__mesh.event.emitter.on('handle-job-ok', function (results) {

              completedJobCount++;

              if (completedJobCount % UPDATE_MOD == 0) console.log('completed:' + completedJobCount.toString() + ' out of ' + N);

              if (completedJobCount >= N) {

                if (completedAlready) return;

                completed = Date.now();

                var completedIn = completed - started;

                console.log('completed in ' + completedIn + ' milliseconds.');

                if (completedIn >= T + 3000) return finish(new Error('not completed within the specified timeframe of ' + T + ' milliseconds'));

                return finish();
              }
            }, function (e) {

              if (e) return reject(e);

              resolve();
            });

          });
        });
      })
      .then(function () {

        return subscriberService.__mesh.exchange.feed.upsert(feedData);
      })
      .then(function (feed) {

        feedId = feed.id;

        var subscriberMesh = subscriberService.__mesh._mesh;

        started = Date.now();

        async.times(N, function (time, timeCB) {

          var random = Math.floor(Math.random() * 100).toString();

          subscriberMesh.data.set('/device/1/' + random, {test: random}, function (e) {

            if (e) return timeCB(e);

            setJobCount++;

            if (setJobCount % UPDATE_MOD == 0) console.log('set:' + setJobCount.toString() + ' out of ' + N);

            timeCB();

          }, finish);
        });

        //go back up to the second step worker listens on emitter
      })
      .catch(finish);

  });

  var METRICS_N = 100;

  var METRICS_T = 10000;

  var METRICS_UPDATE_MOD = 10;

  it('does ' + METRICS_N + ' or more jobs in ' + METRICS_T + ' milliseconds with activateMetrics', function (done) {

    this.timeout(T + 10000);

    var queueService = new Service({methodDurationMetrics: true});

    var subscriberService = new Service({methodDurationMetrics: true});

    var emitterService = new Service({methodDurationMetrics: true});

    var feedRandomName = uuid.v4();

    var queueConfig = {
      queue: {
        jobTypes: {
          "emitter": {concurrency: 10}
        }
      }
    };

    var subscriberConfig = {};

    var subscriberWorkerConfig = {

      name: 'happner-emitter-worker',
      queue: {username: '_ADMIN', password: 'happn', port: 55000, jobTypes: ["feed"]},
      data: {
        port: 55001
      }
    };

    var emitterWorkerConfig = {
      name: 'happner-emitter-worker',
      queue: {username: '_ADMIN', password: 'happn', port: 55000, jobTypes: ["emitter"]},
      data: {
        port: 55002
      }
    };

    var feedData = {
      action: 'create',
      name: 'subscriber test ' + feedRandomName,
      datapaths: [
        '/device/1/*',
        '/device/2/*',
        '/device/3/*'
      ],
      state: 2
    };

    var feedId;

    var setJobCount = 0;

    var completedJobCount = 0;

    var startedJobCount = 0;

    var completedAlready = false;

    var started, completed;

    var finish = function (e) {

      if (!completedAlready) {

        completedAlready = true;

        if (e) {

          if (e && !e.message) e = new Error(e.toString());
        }

        var queueAnalytics = queueService.methodAnalyzer.getAnalysis();

        console.log('analysis:::', queueAnalytics);

        queueService.methodAnalyzer.cleanup();

        emitterService.stop()
          .then(function () {
            return queueService.stop();
          })
          .then(function () {
            return subscriberService.stop();
          })
          .then(function () {
            done(e);
          })
          .catch(function (stopError) {

            done(e);
          });
      }
    };

    started = Date.now();

    queueService
      .queue(queueConfig)
      .then(function () {
        return subscriberService.worker(subscriberWorkerConfig);
      })
      .then(function () {
        return emitterService.worker(emitterWorkerConfig);
      })
      .then(function () {
        return emitterService.emitter(emitterWorkerConfig);
      })
      .then(function () {
        return subscriberService.subscriber(subscriberConfig);
      })
      .then(function () {

        return new Promise(function (resolve, reject) {

          emitterService.__mesh.event.emitter.on('handle-job', function (job) {

            startedJobCount++;

            if (startedJobCount % METRICS_UPDATE_MOD == 0) console.log('started:' + startedJobCount.toString() + ' out of ' + METRICS_N);
          });

          emitterService.__mesh.event.emitter.on('handle-job-failed', function (error) {

            finish(new Error(error.message));

          }, function (e) {

            if (e) return reject(e);

            emitterService.__mesh.event.emitter.on('handle-job-ok', function (results) {

              completedJobCount++;

              if (completedJobCount % METRICS_UPDATE_MOD == 0) console.log('completed:' + completedJobCount.toString() + ' out of ' + METRICS_N);

              if (completedJobCount >= METRICS_N) {

                if (completedAlready) return;

                completed = Date.now();

                var completedIn = completed - started;

                console.log('completed in ' + completedIn + ' milliseconds.');

                if (completedIn >= METRICS_T + 3000) return finish(new Error('not completed within the specified timeframe of ' + METRICS_T + ' milliseconds'));

                return finish();
              }
            }, function (e) {

              if (e) return reject(e);

              resolve();
            });

          });
        });
      })
      .then(function () {

        return subscriberService.__mesh.exchange.feed.upsert(feedData);
      })
      .then(function (feed) {

        feedId = feed.id;

        var subscriberMesh = subscriberService.__mesh._mesh;

        started = Date.now();

        async.times(METRICS_N, function (time, timeCB) {

          var random = Math.floor(Math.random() * 100).toString();

          subscriberMesh.data.set('/device/1/' + random, {test: random}, function (e) {

            if (e) return timeCB(e);

            setJobCount++;

            if (setJobCount % METRICS_UPDATE_MOD == 0) console.log('set:' + setJobCount.toString() + ' out of ' + N);

            timeCB();

          }, finish);
        });

        //go back up to the second step worker listens on emitter
      })
      .catch(finish);

  });

});