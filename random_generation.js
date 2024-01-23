const tojson = JSON.stringify

let _randomFun = Math.random; //default to generic random in generator functions

const _savedRandom = Math.random;

function setRandom(lambda) {
    if (typeof lambda == "function") {
        _randomFun = lambda;
    }
}

function resetRandom() {
    Math.random = _savedRandom;
}

function seededRandom(seed, selectedprime) {
    if (this.x !== undefined) return new seededRandom(seed, selectedprime)
    var thisrand = this
    if (typeof seed == "number")
        thisrand.x = seed
    if (typeof thisrand.x === "undefined")
        thisrand.x = (new Date()).valueOf()
    var prime = 16525637
    if (typeof selectedprime == "number")
        prime = parseInt(selectedprime)
    return function() {
        thisrand.x = ((thisrand.x * 47543538.8) % prime)
        if (thisrand.x == prime)
            thisrand.x -= 1 / prime
        return thisrand.x / prime
    }
}

function setRandomSeed(seed, selectedprime) {
    if (typeof selectedprime == "number") {
        _randomFun = seededRandom(seed, selectedprimed)
    } else {
        _randomFun = seededRandom(seed)
    }
}

function randomChar(set, _cnt)
{
    if (_cnt === undefined) _cnt=1;
    var _ret = "";
    var _i;
    for(_i=0;_i<_cnt;_i++) _ret += pick(set);
    return _ret;
}

function randomHex(cnt) {
    if (cnt === undefined) cnt=1;
    return randomChar("0123456789ABCDEF", cnt);
}

function randomLetter(cnt)
{
    if (cnt === undefined) cnt=1;
    return randomChar("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ", cnt);
}

function randomDigit(cnt)
{
    if (cnt === undefined) cnt=1;
    return randomChar("0123456789")
}

function randomString(cnt, charset)
{
    if (cnt === undefined) cnt=Math.floor(_randomFun()*10);
    if (charset === undefined) charset="0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ-=.abcdefghijklmnopqrstuvwxyz"
    return randomChar(charset,cnt);
}

function randomInt(max) {
    if (max === undefined) max = Math.pow(2,32)-1;
    return getRandomInRange(0,max,0)
}

function pick(arr, min, max) {
    if (max === undefined) max = min;
    if (max === undefined) max = 1;
    if (max < min) max = min
    if (max == 1) {
        return arr[Math.floor(_randomFun()*arr.length)];
    } else {
        var ret = [];
        var len = arr.length;
        while (max > 0) {
            max -= 1;
            ret.push(arr[Math.floor(_randomFun()*len)]);
        }
        return ret;
    }
}

function getRandomInRange(from, to, fixed) {
    return (_randomFun() * (to - from) + from).toFixed(fixed) * 1;
}

exports.setRandom = setRandom;
exports.resetRandom = resetRandom;
exports.seededRandom = seededRandom;
exports.setRandomSeed = setRandomSeed;
exports.randomChar = randomChar;
exports.randomHex = randomHex;
exports.randomLetter = randomLetter;
exports.randomString = randomString;
exports.randomInt = randomInt;
exports.pick = pick;
