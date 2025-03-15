const master = require('cluster');
const cluster = require('./cluster.js');
const os = require('os');
const numCPUs = os.cpus().length;
const logger = require('../utils/logger.js');
const EventEmitter = require('events');
const Eris = require('eris');
const { exec } = require('child_process');
const Queue = require('../utils/queue.js');
const ssvn = require("../../../../package.json").version || "Unknown";
const svn = require("../../package.json").version || "Unknown";
const evn = require("../../../eris/package.json").version || "Unknown";

/**
 * 
 * 
 * @class ClusterManager
 * @extends {EventEmitter}
 */
class ClusterManager extends EventEmitter {
	/**
     * Creates an instance of ClusterManager.
     * @param {any} token 
     * @param {any} mainFile 
     * @param {any} options 
     * @memberof ClusterManager
     */
	constructor(token, mainFile, options) {
		super();

		this.shardCount = options.shards || 0;
		this.firstShardID = options.firstShardID || 0;
		this.lastShardID = options.lastShardID || this.shardCount - 1;
		this.clusterCount = options.clusters || numCPUs;
		this.clusterTimeout = options.clusterTimeout * 1000 || 5000;

		this.token = token || false;

		this.clusters = new Map();
		this.workers = new Map();
		this.queue = new Queue();
		this.callbacks = new Map();

		this.options = options;

		this.statsInterval = options.statsInterval || 60 * 1000;
		this.mainFile = mainFile;
		this.name = options.name || 'Eris-Sharder';
		this.guildsPerShard = options.guildsPerShard || 1300;
		this.clientOptions = options.clientOptions || {};

		if (options.stats === true) {
			this.stats = {
				stats: {
					clusters: []
				},
				clustersCounted: 0
			};
		}

		if (this.token) {
			this.eris = new Eris(token);
			this.launch(false);
		} else {
			throw new Error('No token provided');
		}
	}

	isMaster() {
		return master.isMaster;
	}

	startStats() {
		if (this.statsInterval != null) {
			setInterval(() => {
				this.stats.stats.botStats = {};
				this.stats.stats.clusters = [];
				this.stats.clustersCounted = 0;

				let clusters = Object.entries(master.workers);

				this.executeStats(clusters, 0);
			}, this.statsInterval);
		}
	}

	/**
     * 
     * 
     * @param {any} start 
     * @memberof ClusterManager
     */
	executeStats(clusters, start) {
		const clusterToRequest = clusters.filter((c) => c[1].state === 'online')[start];
		if (clusterToRequest) {
			let c = clusterToRequest[1];

			c.send({ name: 'stats' });

			this.executeStats(clusters, start + 1);
		}
	}

	/**
     * 
     * 
     * @param {any} amount 
     * @param {any} numSpawned 
     * @memberof ClusterManager
     */
	start(clusterID) {
		if (clusterID === this.clusterCount) {
			logger.info('Cluster Manager', 'Clusters have been launched');

			let shards = [];

			for (let i = this.firstShardID; i <= this.lastShardID; i++) {
				shards.push(i);
			}

			let chunkedShards = this.chunk(shards, this.clusterCount);

			chunkedShards.forEach((chunk, clusterID) => {
				let cluster = this.clusters.get(clusterID);

				this.clusters.set(
					clusterID,
					Object.assign(cluster, {
						firstShardID: Math.min(...chunk),
						lastShardID: Math.max(...chunk)
					})
				);
			});

			this.connectShards();
		} else {
			let worker = master.fork();
			this.clusters.set(clusterID, { workerID: worker.id });
			this.workers.set(worker.id, clusterID);
			worker.once('online', () => {
				logger.info('Cluster Manager', `Launched cluster ${clusterID}`);
				clusterID += 1;

				this.start(clusterID);
			});
		}
	}

	/**
     * 
     * 
     * @memberof ClusterManager
     */
	launch() {
		if (master.isMaster) {
			process.on('uncaughtException', (err) => {
				logger.error('Cluster Manager', err.stack);
			});

			this.printLogo();

			process.nextTick(async () => {
				logger.info('General', 'Cluster Manager has started');

				let shards = await this.calculateShards();

				this.shardCount = shards;

				logger.info('Cluster Manager', `Starting ${this.shardCount} shards in ${this.clusterCount} clusters`);

				master.setupMaster({
					silent: false
				});

				// Fork workers.
				this.start(0);
			});
		} else if (master.isWorker) {
			const Cluster = new cluster();
			Cluster.spawn();
		}

		master.on('message', async (worker, message, handle) => {
			if (message.name) {
				const clusterID = this.workers.get(worker.id);

				switch (message.name) {
					case 'log':
						logger.log(`Cluster ${clusterID}`, `${message.msg}`);
						break;
					case 'debug':
						logger.debug(`Cluster ${clusterID}`, `${message.msg}`);
						break;
					case 'info':
						logger.info(`Cluster ${clusterID}`, `${message.msg}`);
						break;
					case 'warn':
						logger.warn(`Cluster ${clusterID}`, `${message.msg}`);
						break;
					case 'error':
						logger.error(`Cluster ${clusterID}`, `${message.msg}`);
						break;
					case 'shardsStarted':
						this.queue.queue.splice(0, 1);

						if (this.queue.queue.length > 0) {
							setTimeout(() => this.queue.executeQueue(), this.clusterTimeout);
						}
						break;
					case 'cluster':
						break;
					case 'shard':
						break;
					case 'stats':
						let tempClusterStats = {cluster: clusterID};
						for (const s in message.stats.botStats) {
							if(["guilds", "uptime", "unavailableGuilds"].includes(s)) {
								tempClusterStats[s] = message.stats.botStats[s]; // Add stat to cluster stats
							}

							if(["uptime"].includes(s)) continue; // Prevents stat from being added to total/global stat
							if(typeof message.stats.botStats[s] === "object") {
								if(!this.stats.stats.botStats[s]) {
									this.stats.stats.botStats[s] = {};
								}
								for (const d in message.stats.botStats[s]) {
									if(!this.stats.stats.botStats[s][d]) {
										this.stats.stats.botStats[s][d] = message.stats.botStats[s][d];
									} else {
										this.stats.stats.botStats[s][d] += message.stats.botStats[s][d];
									}
								}
							} else {
								if(!this.stats.stats.botStats[s]) {
									this.stats.stats.botStats[s] = 0;
								}
								this.stats.stats.botStats[s] += message.stats.botStats[s];
							}
						}

						tempClusterStats.shardsStats = message.stats.shardsStats;
						this.stats.stats.clusters.push(tempClusterStats);
						this.stats.clustersCounted += 1;

						if (this.stats.clustersCounted === this.clusters.size) {
							function compare(a, b) {
								if (a.cluster < b.cluster) return -1;
								if (a.cluster > b.cluster) return 1;
								return 0;
							}

							const clusters = this.stats.stats.clusters.sort(compare);
							this.stats.stats.botStats.clusters = clusters

							this.emit('stats', this.stats.stats.botStats);
						}
						break;

					case 'fetchUser':
						this.fetchInfo(0, 'fetchUser', message.id);
						break;
					case 'fetchGuild':
						this.fetchInfo(0, 'fetchGuild', message.id);
						break;
					case 'fetchChannel':
						this.fetchInfo(0, 'fetchChannel', message.id);
						break;
					case 'fetchMember':
						this.fetchInfo(0, 'fetchMember', [ message.guildID, message.memberID ]);
						break;
					case 'fetchCheck':
						this.fetchInfo(0, 'fetchCheck', message.id);
						break;
					case 'fetchReturn':
						this.fetchReturn(message.value.id, message.value);
						break;
					case 'broadcast':
						this.broadcast(0, message.msg);
						break;
					case 'send':
						this.sendTo(message.cluster, message.msg);
						break;
					case 'WebhookPOST':
						if(message.url && message.message) {
							const url = '/' + new URL(message.url).pathname.split('/').slice(2).join("/")
							this.eris.requestHandler.request("POST", url, false, message.message).catch(err => console.log(err))
						};
						break;
					case 'apiRequest':
						let response;
						let error;

						let { method, url, auth, body, file, _route, short } = message;

						if (file && file.file) file.file = Buffer.from(file.file, 'base64');

						try {
							response = await this.eris.requestHandler.request(
								method,
								url,
								auth,
								body,
								file,
								_route,
								short
							);
						} catch (err) {
							error = {
								code: err.code,
								message: err.message,
								stack: err.stack
							};
						}

						if (error) {
							this.sendTo(clusterID, { _eventName: `apiResponse.${message.requestID}`, err: error });
						} else {
							this.sendTo(clusterID, { _eventName: `apiResponse.${message.requestID}`, data: response });
						}

						break;
				}
			}
		});

		master.on('disconnect', (worker) => {
			const clusterID = this.workers.get(worker.id);
			logger.warn('Cluster Manager', `cluster ${clusterID} disconnected`);
		});

		master.on('exit', (worker, code, signal) => {
			if (this.queue.queue.length > 0) {
				this.queue.queue = this.queue.queue.filter((i) => i.value.id !== this.workers.get(worker.id));
			}
			
			this.restartCluster(worker, code, signal);
		});

		this.queue.on('execute', (item) => {
			let cluster = this.clusters.get(item.item);

			if (cluster) {
				master.workers[cluster.workerID].send(item.value);
			}
		});
	}

	chunk(shards, clusterCount) {
		if (clusterCount < 2) return [ shards ];

		let len = shards.length;
		let out = [];
		let i = 0;
		let size;

		if (len % clusterCount === 0) {
			size = Math.floor(len / clusterCount);

			while (i < len) {
				out.push(shards.slice(i, (i += size)));
			}
		} else {
			while (i < len) {
				size = Math.ceil((len - i) / clusterCount--);
				out.push(shards.slice(i, (i += size)));
			}
		}

		return out;
	}

	connectShards() {
		for (let clusterID in [ ...Array(this.clusterCount).keys() ]) {
			clusterID = parseInt(clusterID);

			let cluster = this.clusters.get(clusterID);

			if (!cluster.hasOwnProperty('firstShardID')) break;

			this.queue.queueItem({
				item: clusterID,
				value: {
					id: clusterID,
					clusterCount: this.clusterCount,
					name: 'connect',
					firstShardID: cluster.firstShardID,
					lastShardID: cluster.lastShardID,
					maxShards: this.shardCount,
					token: this.token,
					file: this.mainFile,
					clientOptions: this.clientOptions,
					processName: this.name
				}
			});
		}

		logger.info('Cluster Manager', `All shards spread`);

		if (this.stats) {
			this.startStats();
		}
	}

	printLogo() {
		const logo = require('asciiart-logo');
		console.log(
            logo({
                name: 'ServerStats',
                font: 'Big',
                padding: 5,
				margin: 3
            })
                .emptyLine()
                .center(`Eris (V${evn})`)
                .center(`Sharder (V${svn})`)
                .center(`${this.name} (V${ssvn})`)
                .render()
		);
	}

	restartCluster(worker, code, signal) {
		const clusterID = this.workers.get(worker.id);

		logger.warn('Cluster Manager', `cluster ${clusterID} died`);

		let cluster = this.clusters.get(clusterID);

		let shards = cluster.shardCount;

		let newWorker = master.fork();
		let SpawnTimeOut = setTimeout(() => {
			let DeadClusters = 0;
			this.clusters.forEach((cluster) => {
				if (!master.workers[cluster.workerID]) DeadClusters++;
			});
			if (DeadClusters === 3 && os.platform() === 'linux') {
				logger.error('Cluster Manager', 'Server is restarting');
				setTimeout(() => {
					exec('sudo apt update && sudo apt upgrade -y && reboot', (error) => {
						if (error) logger.error('Cluster Manager', error);
					});
				}, 15000);
			}
		}, 30000);

		newWorker.once('online', () => {
			this.workers.delete(worker.id);

			this.clusters.set(clusterID, Object.assign(cluster, { workerID: newWorker.id }));

			this.workers.set(newWorker.id, clusterID);

			clearTimeout(SpawnTimeOut);
			logger.debug('Cluster Manager', `Restarting cluster ${clusterID}`);

			this.queue.queueItem({
				item: clusterID,
				value: {
					id: clusterID,
					clusterCount: this.clusterCount,
					name: 'connect',
					shards: shards,
					firstShardID: cluster.firstShardID,
					lastShardID: cluster.lastShardID,
					maxShards: this.shardCount,
					token: this.token,
					file: this.mainFile,
					clientOptions: this.clientOptions,
					processName: this.name
				}
			});
		});
	}

	async calculateShards() {
		let shards = this.shardCount;

		if (this.shardCount !== 0) return Promise.resolve(this.shardCount);

		let result = await this.eris.getBotGateway();
		shards = result.shards;

		if (shards === 1) {
			return Promise.resolve(shards);
		} else {
			let guildCount = shards * 1000;
			let guildsPerShard = this.guildsPerShard;
			let shardsDecimal = guildCount / guildsPerShard;
			let finalShards = Math.ceil(shardsDecimal);
			return Promise.resolve(finalShards);
		}
	}

	fetchInfo(start, type, value) {
		let requestId = Array.isArray(value) ? value[1] : value;
		if (start === 0) {
			this.callbacks.set(requestId, {
				pendingClusters: this.clusters.size,
				timeout: setTimeout(() => {
					if (this.callbacks.has(requestId)) {
						let callback = this.callbacks.get(requestId);
						let targetCluster = this.clusters.get(callback.cluster);
						if (targetCluster && master.workers[targetCluster.workerID]) {
							master.workers[targetCluster.workerID].send({
								name: 'fetchReturn',
								id: requestId,
								value: null
							});
						}
						this.callbacks.delete(requestId);
					}
				}, 10000),
				cluster: start
			});
		}
	
		let cluster = this.clusters.get(start);
		if (cluster) {
			master.workers[cluster.workerID].send({ name: type, value: value });
			this.fetchInfo(start + 1, type, value);
		}
	}
	
	fetchReturn(UUID, value) {
		if (this.callbacks.has(UUID)) {
			let callback = this.callbacks.get(UUID);
			if (value) {
				clearTimeout(callback.timeout);
				this.callbacks.delete(UUID);
				let targetCluster = this.clusters.get(callback.cluster);
				if (targetCluster && master.workers[targetCluster.workerID]) {
					master.workers[targetCluster.workerID].send({
						name: 'fetchReturn',
						id: UUID,
						value: value
					});
				}
			} else {
				callback.pendingClusters--;
				if (callback.pendingClusters <= 0) {
					clearTimeout(callback.timeout);
					this.callbacks.delete(UUID);
					let targetCluster = this.clusters.get(callback.cluster);
					if (targetCluster && master.workers[targetCluster.workerID]) {
						master.workers[targetCluster.workerID].send({
							name: 'fetchReturn',
							id: UUID,
							value: null
						});
					}
				}
			}
		}
	}

	broadcast(start, message) {
		let cluster = this.clusters.get(start);
		if (cluster) {
			if (master.workers[cluster.workerID]) {
				master.workers[cluster.workerID].send(message);
			}
			this.broadcast(start + 1, message);
		}
	}

	sendTo(cluster, message) {
		let worker = master.workers[this.clusters.get(cluster).workerID];
		if (worker) {
			worker.send(message);
		}
	}
}

module.exports = ClusterManager;
