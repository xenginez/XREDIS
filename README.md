# redis_client
redis_client is a Redis database C++ client.

redis_client是Redis数据库的C++客户端。

## Features
-------
* A single header file.
* 仅单头文件。
* Support for Publish and Subscribe.
* 支持发布和订阅。
* All network frameworks are supported.
* 支持所有网络框架。

## Usage
-------
``` C++
#include "redis_client.hpp"

int main()
{
	asio::io_context io;
	std::string buf; buf.resize( 512 );
	asio::ip::tcp::socket socket( io );
	socket.connect( asio::ip::tcp::endpoint( asio::ip::address::from_string( "127.0.0.1" ), 6379 ) );

	redis::client c( [&]( std::string_view data ) { socket.write_some( asio::buffer( data.data(), data.size() ) ); } );

	c.ping( []( redis::value result ) { std::cout << result.get_string() << std::endl; } );
	c.input( buf.begin(), buf.begin() + socket.read_some( asio::buffer( buf ) ) );

	return 0;
}
``` 