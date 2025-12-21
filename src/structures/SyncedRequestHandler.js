const crypto = require('crypto');

class SyncedRequestHandler {
    constructor(ipc, options) {
        this.ipc = ipc;
        this.timeout = options.timeout + 1000;
    }

    request(method, url, auth, body, file, _route, short) {
        return new Promise((resolve, reject) => {
            let stackCapture = new Error().stack;
            let requestID = crypto.randomBytes(16).toString('hex');

            if (file && file.file) file.file = Buffer.from(file.file).toString('base64');

            const cleanup = () => {
                clearTimeout(timeout);
                this.ipc.unregister(`apiResponse.${requestID}`);
            };

            let timeout = setTimeout(() => {
                cleanup();
                reject(new Error(`Request timed out (>${this.timeout}ms) on ${method} ${url} [ID: ${requestID}]`));
            }, this.timeout);

            this.ipc.register(`apiResponse.${requestID}`, data => {
                try {
                    if (data.err) {
                        let error = new Error(data.err.message || 'API request failed');
                        error.stack = (data.err.stack || '') + '\n' + stackCapture.substring(stackCapture.indexOf('\n') + 1);
                        error.code = data.err.code || 0;
                        reject(error);
                    } else {
                        resolve(data.data);
                    }
                } catch (err) {
                    reject(new Error(`Response handler error: ${err.message}`));
                } finally {
                    cleanup();
                }
            });

            process.send({ name: 'apiRequest', requestID, method, url, auth, body, file, _route, short });
        });
    }
}

module.exports = SyncedRequestHandler;