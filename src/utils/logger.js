const { Console } = require('console');
const colors = require('colors');
const timestamp = require('time-stamp');

const console = new Console({
	stdout: process.stdout,
	stderr: process.stderr,
	colorMode: false
});

colors.setTheme({
    silly: 'rainbow',
    log: 'grey',
    verbose: 'cyan',
    prompt: 'grey',
    info: 'green',
    data: 'grey',
    help: 'cyan',
    warn: 'yellow',
    debug: 'cyan',
    error: 'red'
});

/**
 * 
 * 
 * @class Logger
 */
class Logger {
    constructor() {

    }

    /**
     * 
     * 
     * @param {any} source 
     * @param {any} msg 
     * @memberof Logger
     */
	log(source, msg) {
		let message = colors.log(msg);
		console.log(`${createSource(source)} | ${message}`);
	}

    /**
     * 
     * 
     * @param {any} source 
     * @param {any} msg 
     * @memberof Logger
     */
	info(source, msg) {
		let message = colors.info(msg);
		console.info(`${createSource(source)} | ${message}`);
	}

    /**
     * 
     * 
     * @param {any} source 
     * @param {any} msg 
     * @memberof Logger
     */
	warn(source, msg) {
		let message = colors.warn(msg);
		console.warn(`${createSource(source)} | ${message}`);
	}

    /**
     * 
     * 
     * @param {any} source 
     * @param {any} msg 
     * @memberof Logger
     */
	error(source, msg) {
		let message = colors.error(msg);
		console.error(`${createSource(source)} | ${message}`);
	}

    /**
     * 
     * 
     * @param {any} source 
     * @param {any} msg 
     * @memberof Logger
     */
	data(source, msg) {
		let message = colors.data(msg);
		console.log(`${createSource(source)} | ${message}`);
	}

    /**
     * 
     * 
     * @param {any} source 
     * @param {any} msg 
     * @memberof Logger
     */
	debug(source, msg) {
		let message = colors.debug(msg);
		console.debug(`${createSource(source)} | ${message}`);
	}
}

module.exports = new Logger();

/**
* 
* 
* @param {any} source 
* @returns {String} Date now and source with padding.
*/
const createSource = (source) => {
	return `${timestamp(`YYYY/MM/DD HH:mm:ss`)} | ${source.padEnd(15)}`;
};