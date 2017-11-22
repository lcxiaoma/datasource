<?php 
use Workerman\Connection\AsyncTcpConnection;
use Workerman\Lib\Timer;

function rsyncConnect($TCP_INFO,$link,$memcache,$connection){
	$connection_to_data = new AsyncTcpConnection($TCP_INFO);
	$connection_to_data->onConnect = function($connection_to_data)
	{
		 global $timer_id;
		 $timer_id = Timer::add(1, function()use($connection_to_data){
			$time_now = time();
			if (empty($connection_to_data->lastMessageTime)) {
				$connection_to_data->lastMessageTime = $time_now;
			}
			$multi = $time_now - $connection_to_data->lastMessageTime;
			if ($multi >= 30&& !is_close()) {//30s没有收到数据源并且处于开盘时间 重连 
				$connection_to_data->close();
			}
		});
		$connection_to_data->send("Login");
	};
	$connection_to_data->onMessage = function($connection_to_data, $buffer)use($connection,$link,$memcache)
	{
		$connection_to_data->lastMessageTime = time();
		global $DB_TYPE;
		$arr=explode(';',$buffer);
		mysql_query("BEGIN");
		foreach($arr as $value){
			if($value){
				$arr=explode(':',$value);
				$option = $arr[0];
				$price  = $arr[1];
				 //if(getOption($option,$link))
				 //{
					$price = RiskControl($option,$price,$link); //进行风控
					insertCurrent ($option,$price,$link); //更新资产当前数据
					 if($DB_TYPE=="memcache") $db_info = $memcache; else $db_info = $link;
					 insertData_new($option,$price,array(1,5,15,30,60,240,1440,10080,43200),$DB_TYPE,$db_info); //绘制k线图数据
				 //}
				// var_dump($option.":".$price);
				foreach($connection->connections as $connection_)
				{
					$connection_->send($option.":".$price);
				}
			}
		}
		mysql_query("COMMIT");
	};
	$connection_to_data->onClose = function($connection_to_data)
	{
		global $timer_id;
		if($timer_id){
			Timer::del($timer_id);
		}
		error_log(date('Y-m-d H:i:s', time()).":onClose\n",3,'errors.log');
		$connection_to_data->reConnect(10);
	};
	$connection_to_data->connect();	
}

function is_close()
{
	if(date("w")=="6"&&date("H")>=6){
		return true;
	}elseif(date("w")=="0"){
		return true;
	}elseif(date("w")=="1"&&date("H")<7){
		return true;
	}
	return false;
}

function insertCurrent($option,$price,$link){
	$result = mysql_query("select option_key from sh_current where option_key ='$option';",$link);
	$current_time = time();
	if($result){
		if(mysql_affected_rows()){
			if($row = mysql_fetch_assoc($result))
			{
				$sql = "update sh_current set value=$price,time=$current_time where option_key ='$option';";
			}
		}else{
			$sql = "insert into sh_current(option_key,value,time)values('$option',$price,$current_time);";
		}
		mysql_query($sql,$link);
	}
	mysql_free_result($result);
}

function insertData($option,$price,$cycle,$db_type,$link)
{
	$current_time = time();
	$key = $option.(floor($current_time/$cycle/60)*$cycle*60)."_".$cycle;
	if($db_type=="mysql"){
		$result = mysql_query("select id,high,low from sh_record where option_key ='$key';",$link);
		if($result){
			if(mysql_affected_rows()){
				if($row = mysql_fetch_assoc($result))
				{
					$str ="update sh_record set ";
					if($price>$row['high']){
						$str .="high = $price ,";
					}
					if($price<$row['low']){
						$str .="low = $price ,";
					}
					$str .="close = $price,create_time=$current_time where option_key ='$key'";
					mysql_query($str,$link);
				}
			}else{
				mysql_query("insert into sh_record(option_key,open,high,low,close,create_time)values('$key',$price,$price,$price,$price,$current_time);",$link);
			}
		}
		mysql_free_result($result);
	}else{//memcache存储
		$value = $link->get($key);
		if(empty($value)){
			$link->set($key,$price.",".$price.",".$price.",".$price);
		}else{
			$array_value = explode(',', $value);
			if(count($array_value)==4) {
				$array_value[3] = $price;
				// 最高点 
				if($array_value[1] < $price) {
					$array_value[1] = $price;
				}
				// 最低点
				if($array_value[2] > $price) {
					$array_value[2] = $price;
				}
				$lastList = implode(',', $array_value);
				$link->set($key, $lastList); 
			}
		}
	}
}
//开高低收
function insertMemcacheData($option,$price,$cycle,$memcache)
{
	$current_time = time();
	$key = $option.(floor($current_time/$cycle/60)*$cycle*60)."_".$cycle;
	$value = $memcache->get($key);
	if(empty($value)){
		$memcache->set($key,$price.",".$price.",".$price.",".$price);
	}else{
		$array_value = explode(',', $value);
		if(count($array_value)==4) {
			$array_value[3] = $price;
			// 最高点 
			if($array_value[1] < $price) {
				$array_value[1] = $price;
			}
			// 最低点
			if($array_value[2] > $price) {
				$array_value[2] = $price;
			}
			$lastList = implode(',', $array_value);
			$memcache->set($key, $lastList); 
		}
	}
}

function RiskControl($option,$price,$link)
{
	$result = mysql_query("select digits,risk_point from sh_riskcontrol where option_key ='$option' and type =1;",$link);
	if($result){
		if(mysql_affected_rows()){
			if($row = mysql_fetch_assoc($result))
			{
				$price = $price+$row['risk_point']*pow(10,-$row['digits']);
			}
		}
	}
	mysql_free_result($result);
	return $price;
}

function insertData_new($option,$price,$cycle = array(),$db_type,$link)
{
	foreach ($cycle as $cycle_){ 
		$current_time = time();
		$key = $option.(floor($current_time/$cycle_/60)*$cycle_*60)."_".$cycle_;
		if($db_type=="mysql"){
			$result = mysql_query("select id,high,low from sh_record where option_key ='$key';",$link);
			if($result){
				if(mysql_affected_rows()){
					if($row = mysql_fetch_assoc($result))
					{
						$str ="update sh_record set ";
						if($price>$row['high']){
							$str .="high = $price ,";
						}
						if($price<$row['low']){
							$str .="low = $price ,";
						}
						$str .="close = $price,create_time=$current_time where option_key ='$key'";
						mysql_query($str,$link);
					}
				}else{
					mysql_query("insert into sh_record(option_key,open,high,low,close,create_time)values('$key',$price,$price,$price,$price,$current_time);",$link);
				}
			}
			mysql_free_result($result);
		}else{//memcache存储
			$value = $link->get($key);
			
			if(empty($value)){
				$link->set($key,$price.",".$price.",".$price.",".$price);
			}else{
				$array_value = explode(',', $value);
				if(count($array_value)==4) {
					$array_value[3] = $price;
					// 最高点 
					if($array_value[1] < $price) {
						$array_value[1] = $price;
					}
					// 最低点
					if($array_value[2] > $price) {
						$array_value[2] = $price;
					}
					$lastList = implode(',', $array_value);
					$link->set($key, $lastList); 
				}
			}
		}	
    }
}

function getOption($option,$link){
	$result = mysql_query("select id from sh_option where option_key ='$option' and status = 1 ;",$link);
	if($result){
		if(mysql_affected_rows()){
			return true;
		}
	}
	mysql_free_result($result);
	return false;
}