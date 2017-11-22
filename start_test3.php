<?php 
//debug模式 安装redis
//正式模式  安装redis 优化linux内核  使用event或者libevent扩展   //if(strstr($result,"EURAUD"))
use Workerman\Worker;
use Workerman\Connection\AsyncTcpConnection;
use Workerman\Lib\Timer;
use \GatewayWorker\Lib\Db;
require_once __DIR__ . '/Autoloader.php';
require_once __DIR__ . '/function.php';
require_once __DIR__ . '/config.php';
//==================接受数据源===========================
$tcp = new Worker("tcp://0.0.0.0:2347");
$tcp->count = 1;
$tcp->name = "datasource";
$tcp->onWorkerStart = function($connection)
{
	global $DB_TYPE,$MYSQL_IP,$MYSQL_USER,$MYSQL_PWD,$MYSQL_DB,$TCP_INFO,$TCP_INFO2;
	if(!@$link){
		$link = mysql_connect($MYSQL_IP,$MYSQL_USER, $MYSQL_PWD);
		mysql_select_db($MYSQL_DB, $link);
		mysql_query("set names utf8");
	}
	$memcache = null;
	if($DB_TYPE=="memcache"){
		global $MEMCACHE_IP,$MEMCACHE_PORT;
		$memcache = new Memcache();
		$memcache->pconnect($MEMCACHE_IP,$MEMCACHE_PORT);
	}
	//=============连接数据源1
	rsyncConnect($TCP_INFO,$link,$memcache,$connection);
	//=============连接数据源2
	rsyncConnect($TCP_INFO2,$link,$memcache,$connection);
};

// 运行worker
Worker::runAll();