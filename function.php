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

//插入trade表 正式下单
function trade_real($gd_id,$trade_point){
		//$trade = M('trade');
		//$trade->startTrans();
		
		//$gd_info = M('guadan') -> where('id = '.$gd_id)->find();//挂单信息
		//$referid = M('user')->where('id ='.$gd_info['user_id'])->find();
		
		$sql ="select * from sh_guadan where id = ".$gd_id; //SQL语句 
		$result = mysql_query($sql);			
		$gd_info = mysql_fetch_array($result);
		
		$sql ="select * from sh_user where id = ".$$gd_info['user_id']; //SQL语句 
		$result = mysql_query($sql);			
		$referid = mysql_fetch_array($result);
		
		
		
		$trde['user_id'] = $gd_info['user_id'];
		$trde['referid'] = $referid['referid'];
		$trde['trade_amount'] = $gd_info['trade_amount'];
		$trde['trade_direction'] = $gd_info['trade_direction'];
		$trde['trade_point'] = $trade_point;//点位
		$trde['trade_time'] = time();
		$trde['symbol'] = $gd_info['symbol'];
		$trde['symbol_show'] = $gd_info['symbol_show'];
		$trde['digits'] = $gd_info['digits'];
		$trde['price'] = $gd_info['price'];
		$trde['open_balance'] = $referid['money'];
		$trde['closed'] = 0;
		$trde['stop_loss'] = $gd_info['stop_loss'];
		$trde['stop_win'] = $gd_info['stop_win'];
		$trde['fee'] = $gd_info['fee'];
		$trde['pruduct_type'] = $gd_info['pruduct_type'];
		$trde['log'] = '挂单成交'.$gd_id;
		$trde['amount'] = $gd_info['amount'];
		$trde['gd_id'] = $gd_id;
		
		
		$sql ="INSERT INTO  sh_trade (user_id,referid,trade_amount,trade_direction,trade_point,trade_time,symbol,symbol_show,digits,price,open_balance,closed,stop_loss,stop_win,fee,pruduct_type,log,amount,gd_id) value (";
		$sql .= "'".$trde['user_id']."',";
		$sql .= "'".$trde['referid']."',";
		$sql .= "'".$trde['trade_amount']."',";
		$sql .= "'".$trde['trade_direction']."',";
		$sql .= "'".$trde['trade_point']."',";
		$sql .= "'".$trde['trade_time']."',";
		$sql .= "'".$trde['symbol']."',";
		$sql .= "'".$trde['symbol_show']."',";
		$sql .= "'".$trde['digits']."',";
		$sql .= "'".$trde['price']."',";
		$sql .= "'".$trde['open_balance']."',";
		$sql .= "'".$trde['closed']."',";
		$sql .= "'".$trde['stop_loss']."',";
		$sql .= "'".$trde['stop_win']."',";
		$sql .= "'".$trde['fee']."',";
		$sql .= "'".$trde['pruduct_type']."',";
		$sql .= "'".$trde['log']."',";
		$sql .= "'".$trde['amount']."',";
		$sql .= "'".$trde['gd_id']."')";
		$trade_add = mysql_query($sql);
		
	}
	

function jiesuan($order,$bid,$earn,$comment,$earn_all = 0){

		$date = date('Y-m-d H:i:s');

		
		mysql_query('start transaction');
	
		$sql ="select * from sh_user where id = ".$order['user_id']; //SQL语句
		 
		$result = mysql_query($sql);	
		
		$uinfo = mysql_fetch_array($result);
		

		$udata['money_freeze'] = $uinfo['money_freeze'] - $order['trade_amount']; //冻结余额
		if($udata['money_freeze']<0)
		{
			$udata['money_freeze'] = 0;
		}
		$udata['money'] = $uinfo['money']+ $order['trade_amount'] + $earn; //可用余额
		if($udata['money']< 0 )
		{
			$udata['money'] = 0;
			$data['profit']  = 0-($uinfo['money']+ $order['trade_amount']);
		}
		else
		{
			$data['profit']  = $earn;
		}
		
		$sql ="update  sh_user set money = '".$udata['money']."',money_freeze = '".$udata['money_freeze']."' where id =".$order['user_id']; //SQL语句
		 
		$ref1 = mysql_query($sql);	

		
		//更改trades表
		//$data['profit']      = $earn;
		$data['profit']  = $earn;
		$data['close_price'] = $bid;
		$data['close_time']  = $date;
		$data['log']    = $comment;
		$data['closed']     = 1;
		$data['earn_all']    = $earn_all;
		//dump($data);die;
		//$ref = $trade->where("id = ".$order['id'])->save($data); 
		
		$sql ="update sh_trade set ";
		$sql .= "profit = '".$data['profit']."',";
		$sql .= "close_price = '".$data['close_price']."',";
		$sql .= "close_time = '".$data['close_time']."',";
		$sql .= "log = '".$data['log']."',";
		$sql .= "closed = '".$data['closed']."',";
		$sql .= "earn_all = '".$data['earn_all']."'";
		$sql .= "where id =".$order['id'];
		$ref = mysql_query($sql);
		

		//更改user_cash_account表
		$cash['user_id']=session("user_uid");
		$cash['user_name']=$uinfo['uname'];
		$cash['pre_money']=$uinfo['money'];
		$cash['pre_freeze_money']=$uinfo['money_freeze'];
		$cash['option_money']=$earn;
		$cash['finish_money']=$udata['money'];
		$cash['finish_freeze_money']=$udata['money_freeze'];
		$cash['option_time']=time();
		$cash['log']=$comment;
		//$ref2 = M('user_cash_account') -> add($cash);
		
		$sql ="INSERT INTO  sh_user_cash_account (user_id,user_name,pre_money,pre_freeze_money,option_money,finish_money,finish_freeze_money,option_time,log) value (";
		$sql .= "'".$cash['user_id']."',";
		$sql .= "'".$cash['user_name']."',";
		$sql .= "'".$cash['pre_money']."',";
		$sql .= "'".$cash['pre_freeze_money']."',";
		$sql .= "'".$cash['option_money']."',";
		$sql .= "'".$cash['finish_money']."',";
		$sql .= "'".$cash['finish_freeze_money']."',";
		$sql .= "'".$cash['option_time']."',";
		$sql .= "'".$cash['log']."')";
		$ref2 = mysql_query($sql);

		
		//返佣
		//$ref3 = $this->rebate($order,$uinfo);  //暂时无用
		$ref3 = true;
		
		
		
		//更改sh_proxy表的保证金  //暂时无用
		//$ainfo =  $admin->where("id = ".$uinfo['referid'])->find();
		// $sql ="select * from sh_proxy where id = ".$uinfo['referid']; //SQL语句
 
		// $result = mysql_query($sql,$conn);	
		
		// $ainfo = mysql_fetch_array($result);
		// if(!empty($ainfo))
		// {
			// $adata["money"] = $ainfo["money"] - $earn;
			// $ref4 = $admin->where("id = ".$uinfo['referid'])->save($adata);
		// }
		// else
		// {
			// $ref4 = true;
		// }
		
		$ref4 = true;
		
		
		
		
		if($ref && $ref1 && $ref2 && $ref3 && $ref4)//成功
		{
			mysql_query("COMMIT"); 
			//echo "平仓成功";
		}
		else{
			mysql_query("ROLLBACK"); 
			//echo "平仓失败";
		}		
	
	
	
	
}