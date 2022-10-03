const Eris = require("eris");
const Base = require("../structures/Base.js");
const SyncedRequestHandler = require('../structures/SyncedRequestHandler.js');
const { inspect } = require('util');
const IPC = require("../structures/IPC.js");

/**
 * 
 * 
 * @class Cluster
 */
class Cluster {

    /**
     * Creates an instance of Cluster.
     * @param {any} clusterID 
     * @memberof Cluster
     */
    constructor() {
		this.shards = 0;
		this.maxShards = 0;
		this.firstShardID = 0;
		this.lastShardID = 0;
		this.mainFile = null;
		this.clusterID = 0;
        this.clusterCount = 0;
        this.guilds = 0;
        this.unavailableGuilds = 0;
        this.slashCommands = 0;
        this.fetchedServers = 0;
        this.needUpdateCount = 0,
		this.hasUpdatedCount = 0,
        this.counterUpdates = 0;
        this.eventMisses = 0;
        this.failedUpdates = 0;
        this.dispatchs = {};
		this.clusterUptime = 0;
		this.botUptime = 0;
		this.shardsStats = [];
		this.app = null;
		this.bot = null;

        this.ipc = new IPC();

        console.log = (str) => process.send({ name: "log", msg: this.logOverride(str) });
        console.error = (str) => process.send({ name: "error", msg: this.logOverride(str) });
        console.warn = (str) => process.send({ name: "warn", msg: this.logOverride(str) });
        console.info = (str) => process.send({ name: "info", msg: this.logOverride(str) });
        console.debug = (str) => process.send({ name: "debug", msg: this.logOverride(str) });

    }

    logOverride(message) {
        if (typeof message == 'object') return inspect(message);
        else return message;
    }

    spawn() {
        process.on('uncaughtException', (err) => {
            process.send({ name: "error", msg: `Uncaught exception at, reason:  ${err.stack}` });
        });

        process.on('unhandledRejection', (reason, p) => {
            process.send({ name: "error", msg: `Unhandled rejection at, promise: ${p} (reason:  ${reason.stack})` });
        });

        process.on('warning', (warn) => {
            process.send({ name: "warn", msg: `Warning at, reason: ${warn.stack}` });
        });

        process.on("message", msg => {
            if (msg.name) {
                switch (msg.name) {
                    case "connect": {
                        this.firstShardID = msg.firstShardID;
                        this.lastShardID = msg.lastShardID;
                        this.mainFile = msg.file;
                        this.clusterID = msg.id;
                        this.clusterCount = msg.clusterCount;
                        this.shards = (this.lastShardID - this.firstShardID) + 1;
                        this.maxShards = msg.maxShards;
                        this.processName = msg.processName;

                        if (this.shards < 1) return;

                        this.connect(msg.firstShardID, msg.lastShardID, this.maxShards, msg.token, "connect", msg.clientOptions);

                        break;
                    }
                    case "stats": {
                        process.send({
                            name: "stats", stats: {
                                guilds: this.guilds,
                                unavailableGuilds: this.unavailableGuilds,
								ram: process.memoryUsage().rss,
								shards: this.shards,
                                slashCommands: this.slashCommands,
                                fetchedServers: this.fetchedServers,
                                needUpdateCount: this.needUpdateCount,
                                hasUpdatedCount: this.hasUpdatedCount,
                                counterUpdates: this.counterUpdates,
                                eventMisses: this.eventMisses,
                                failedUpdates: this.failedUpdates,
                                dispatchs: this.dispatchs,
								clusterUptime: this.clusterUptime,
								botUptime: this.botUptime,
								shardsStats: this.shardsStats
                            }
                        });

                        if (this.slashCommands > 0) {
							this.slashCommands = -1;
						}

                        if (this.counterUpdates > 0) {
							this.counterUpdates = -1;
						}

                        if (this.eventMisses > 0) {
							this.eventMisses = -1;
						}

                        if (this.failedUpdates > 0) {
							this.failedUpdates = -1;
						}

                        if (Object.keys(this.dispatchs).length > 0) {
                            this.dispatchs = {};
                        }

                        break;
                    }
                    case "fetchUser": {
                        if (!this.bot) return;
                        let id = msg.value;
                        let user = this.bot.users.get(id);
                        if (user) {
                            process.send({ name: "fetchReturn", value: user });
                        }

                        break;
                    }
                    case "fetchChannel": {
                        if (!this.bot) return;
                        let id = msg.value;
                        let channel = this.bot.getChannel(id);
                        if (channel) {
                            channel = channel.toJSON();
                            return process.send({ name: "fetchReturn", value: channel });
                        }

                        break;
                    }
                    case "fetchGuild": {
                        if (!this.bot) return;
                        let id = msg.value;
                        let guild = this.bot.guilds.get(id);
                        if (guild) {
                            guild = guild.toJSON();
                            process.send({ name: "fetchReturn", value: guild });
                        }

                        break;
                    }
                    case "fetchMember": {
                        if (!this.bot) return;
                        let [guildID, memberID] = msg.value;

                        let guild = this.bot.guilds.get(guildID);

                        if (guild) {
                            let member = guild.members.get(memberID);

                            if (member) {
                                member = member.toJSON();
                                process.send({ name: "fetchReturn", value: member });
                            }
                        }

                        break;
                    }
                    case "fetchReturn":
                        this.ipc.emit(msg.id, msg.value);
                        break;
                    case "restart":
                        process.exit(1);
                        break;
                }
            }
        });
    }

    /**
     * 
     * 
     * @param {any} firstShardID 
     * @param {any} lastShardID 
     * @param {any} maxShards 
     * @param {any} token 
     * @param {any} type 
     * @memberof Cluster
     */
    connect(firstShardID, lastShardID, maxShards, token, type, clientOptions) {
        process.send({ name: "log", msg: `Connecting with ${this.shards} shard(s)` });

        let options = { autoreconnect: true, firstShardID: firstShardID, lastShardID: lastShardID, maxShards: maxShards, processName: this.processName };
        let optionss = Object.keys(options);
        optionss.forEach(key => {
            delete clientOptions[key];
        });

        Object.assign(options, clientOptions);

        const bot = new Eris(token, options);
        this.bot = bot;

        this.bot.requestHandler = new SyncedRequestHandler(this.ipc, {
            timeout: this.bot.options.requestTimeout
        });

        bot.on("connect", id => {
            process.send({ name: "log", msg: `Shard ${id} established a connection` });
        });

        bot.on("shardDisconnect", (err, id) => {
            process.send({ name: "log", msg: `Shard ${id} has been disconnected${!err ? '' : `, reason: ${err.message}`}` });
        });

        bot.on("shardReady", id => {
            process.send({ name: "log", msg: `Shard ${id} is operational` });
        });

        bot.on("shardResume", id => {
            process.send({ name: "log", msg: `Shard ${id} has been resumed` });
        });

        bot.on("warn", (message, id) => {
            process.send({ name: "warn", msg: `Shard ${id} ${message}` });
        });

        bot.on("error", (error, id) => {
            process.send({ name: "error", msg: `Shard ${id} ${error.message} ${!error.code ? '' : `(${error.code})`}` });
        });

        bot.once("ready", id => {
            this.loadCode(bot);

            this.startStats(bot);
        });

        bot.on("ready", id => {
            process.send({ name: "log", msg: `Shards ${this.firstShardID} - ${this.lastShardID} are now ready` });

            process.send({ name: "shardsStarted" });
        });

        bot.connect();
    }

    loadCode(bot) {
        let rootPath = process.cwd();
        rootPath = rootPath.replace(`\\`, "/");


        let path = `${rootPath}${this.mainFile}`;
        let app = require(path);
        if (app.prototype instanceof Base) {
            this.app = new app({ bot: bot, clusterID: this.clusterID, ipc: this.ipc });
            this.app.launch();
        } else {
            console.error("Your code has not been loaded! This is due to it not extending the Base class. Please extend the Base class!");
        }
    }

    startStats(bot) {
        setInterval(() => {
            if(bot.stats) {
                if (this.slashCommands === -1) {
                    bot.stats.slashCommands = 0;
                }
    
                if (this.counterUpdates === -1) {
                    bot.stats.counterUpdates = 0;
                }

                if (this.eventMisses === -1) {
                    bot.stats.eventMisses = 0;
                }
    
                if (this.failedUpdates === -1) {
                    bot.stats.failedUpdates = 0;
                }

                if (Object.keys(this.dispatchs).length === 0) {
                    bot.stats.dispatchs = {}
                }

                this.slashCommands = bot.stats.slashCommands;
                this.needUpdateCount = bot.stats.needUpdateCount;
                this.hasUpdatedCount = bot.stats.hasUpdatedCount;
                this.fetchedServers = bot.stats.fetchedServers;
                this.counterUpdates = bot.stats.counterUpdates;
                this.eventMisses = bot.stats.eventMisses;
                this.failedUpdates = bot.stats.failedUpdates;
                this.dispatchs = bot.stats.dispatchs;
            }

            this.guilds = bot.guilds.size;
            this.unavailableGuilds = bot.unavailableGuilds.size;
			this.clusterUptime = Math.round(process.uptime() * 1000);
			this.botUptime = bot.uptime;
			this.shardsStats = [];
			this.bot.shards.forEach((shard) => {
				this.shardsStats.push({
					id: shard.id,
                    ready: shard.ready,
					latency: shard.latency,
					status: shard.status
				});
			});
        }, 1000 * 5);
    }
}

module.exports = Cluster;
