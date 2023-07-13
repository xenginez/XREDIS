/*!
 * \file	XREDIS.hpp
 *
 * \author	ZhengYuanQing
 * \date	2023/07/13
 * \email	zhengyuanqing.95@gmail.com
 *
 */
#ifndef XREDIS_HPP__94D2E943_814E_4967_A639_26765ED2C208
#define XREDIS_HPP__94D2E943_814E_4967_A639_26765ED2C208

#include <map>
#include <mutex>
#include <stack>
#include <string>
#include <vector>
#include <variant>
#include <functional>

namespace XREDIS
{
	static constexpr char STRING_PATCH = '+';
	static constexpr char ERROR_MATCH = '-';
	static constexpr char INTEGER_MATCH = ':';
	static constexpr char BULK_MATCH = '$';
	static constexpr char ARRAY_MATCH = '*';
	static constexpr char * CRCF_MATCH = "\r\n";

	class value
	{
		enum error_code
		{
			no_error,
			io_error,
			timeout,
			redis_parse_error,  //redis protocal parse error
			redis_reject_error, //rejected by redis server
			unknown_error
		};

	public:
		value()
			: _value( std::monostate() )
		{
		}

		value( value && other )
			: _value( std::move( other._value ) ), _error_code( other._error_code )
		{
		}

		value( int64_t i )
			: _value( i )
		{
		}

		value( const char * s )
			: _value( std::string( s ) )
		{
		}

		value( const std::string & s )
			: _value( s )
		{
		}

		value( const std::vector<value> & a )
			: _value( a )
		{
		}

		value( int error_code, const std::string & error_msg )
			: _value( error_msg ), _error_code( error_code )
		{

		}

		value( const value & ) = default;

		value & operator=( value && ) = default;

		value & operator=( const value & ) = default;

	public:
		int64_t to_int() const
		{
			if ( is_int() )
				return get_int();
			return 0;
		}

		std::string to_string() const
		{
			if ( is_string() )
				return get_string();
			return {};
		}

		std::vector<value> to_array() const
		{
			if ( is_array() )
				return get_array();
			return {};
		}

		int error_code() const
		{
			return _error_code;
		}

	public:
		bool is_ok() const
		{
			return _error_code == 0;
		}

		bool is_error() const
		{
			return _error_code != 0;
		}

		bool is_io_error() const
		{
			return _error_code == 1;
		}

		bool is_null() const
		{
			return _value.index() == 0;
		}

		bool is_int() const
		{
			return _value.index() == 1;
		}

		bool is_string() const
		{
			return _value.index() == 2;
		}

		bool is_array() const
		{
			return _value.index() == 3;
		}

	public:
		int64_t get_int()
		{
			return std::get<int64_t>( _value );
		}

		int64_t get_int() const
		{
			return std::get<int64_t>( _value );
		}

		std::string & get_string()
		{
			return std::get< std::string >( _value );
		}

		const std::string & get_string() const
		{
			return std::get< std::string >( _value );
		}

		std::vector<value> & get_array()
		{
			return std::get< std::vector< value > >( _value );
		}

		const std::vector<value> & get_array() const
		{
			return std::get< std::vector< value > >( _value );
		}

	public:
		bool operator==( const value & rhs ) const
		{
			return _value == rhs._value;
		}

		bool operator!=( const value & rhs ) const
		{
			return !( _value == rhs._value );
		}

	private:
		int _error_code = 0;
		std::variant< std::monostate, int64_t, std::string, std::vector<value> > _value;
	};

	class parser
	{
		enum state_t
		{
			Start = 0,
			StartArray = 1,

			String = 2,
			StringLF = 3,

			ErrorString = 4,
			ErrorLF = 5,

			Integer = 6,
			IntegerLF = 7,

			BulkSize = 8,
			BulkSizeLF = 9,
			Bulk = 10,
			BulkCR = 11,
			BulkLF = 12,

			ArraySize = 13,
			ArraySizeLF = 14,
		};

	public:
		enum result_t
		{
			Completed,
			Incompleted,
			Error,
		};

	public:
		parser()
			: _bulk_size( 0 )
		{
			_buf.reserve( 64 );
		}

	public:
		value result() const
		{
			return _value;
		}

		template< typename Iterator > std::pair<size_t, result_t> parse( Iterator beg, Iterator end )
		{
			return chunk( beg, end );
		}

	protected:
		template< typename Iterator > std::pair<size_t, result_t> chunk( Iterator beg, Iterator end )
		{
			Iterator cur = beg;
			state_t state = Start;

			if ( !_states.empty() )
			{
				state = _states.top();
				_states.pop();
			}

			while ( cur != end )
			{
				char c = *cur++;

				switch ( state )
				{
				case StartArray:
				case Start:
					_buf.clear();
					switch ( c )
					{
					case STRING_PATCH:
						state = String;
						break;
					case ERROR_MATCH:
						state = ErrorString;
						break;
					case INTEGER_MATCH:
						state = Integer;
						break;
					case BULK_MATCH:
						state = BulkSize;
						_bulk_size = 0;
						break;
					case ARRAY_MATCH:
						state = ArraySize;
						break;
					default:
						return std::make_pair( std::distance( beg, cur ), Error );
					}
					break;
				case String:
					if ( c == '\r' )
					{
						state = StringLF;
					}
					else if ( is_char( c ) && !is_control( c ) )
					{
						_buf.push_back( c );
					}
					else
					{
						std::stack<state_t>().swap( _states );
						return std::make_pair( std::distance( beg, cur ), Error );
					}
					break;
				case ErrorString:
					if ( c == '\r' )
					{
						state = ErrorLF;
					}
					else if ( is_char( c ) && !is_control( c ) )
					{
						_buf.push_back( c );
					}
					else
					{
						std::stack<state_t>().swap( _states );
						return std::make_pair( std::distance( beg, cur ), Error );
					}
					break;
				case BulkSize:
					if ( c == '\r' )
					{
						if ( _buf.empty() )
						{
							std::stack<state_t>().swap( _states );
							return std::make_pair( std::distance( beg, cur ), Error );
						}
						else
						{
							state = BulkSizeLF;
						}
					}
					else if ( isdigit( c ) || c == '-' )
					{
						_buf.push_back( c );
					}
					else
					{
						std::stack<state_t>().swap( _states );
						return std::make_pair( std::distance( beg, cur ), Error );
					}
					break;
				case StringLF:
					if ( c == '\n' )
					{
						state = Start;
						_value = value( _buf );
					}
					else
					{
						std::stack<state_t>().swap( _states );
						return std::make_pair( std::distance( beg, cur ), Error );
					}
					break;
				case ErrorLF:
					if ( c == '\n' )
					{
						state = Start;
						_value = value( _buf );
					}
					else
					{
						std::stack<state_t>().swap( _states );
						return std::make_pair( std::distance( beg, cur ), Error );
					}
					break;
				case BulkSizeLF:
					if ( c == '\n' )
					{
						_bulk_size = to_long( _buf.data(), _buf.size() );
						_buf.clear();

						if ( _bulk_size == -1 )
						{
							state = Start;
							_value = value(); // Nil
						}
						else if ( _bulk_size == 0 )
						{
							state = BulkCR;
						}
						else if ( _bulk_size < 0 )
						{
							std::stack<state_t>().swap( _states );
							return std::make_pair( std::distance( beg, cur ), Error );
						}
						else
						{
							_buf.reserve( _bulk_size );

							size_t available = std::distance( beg, end ) - std::distance( beg, cur );
							size_t canRead = std::min( _bulk_size, available );

							if ( canRead > 0 )
							{
								_buf.assign( cur, cur + canRead );
								cur += canRead;
								_bulk_size -= canRead;
							}


							if ( _bulk_size > 0 )
							{
								state = Bulk;
							}
							else
							{
								state = BulkCR;
							}
						}
					}
					else
					{
						std::stack<state_t>().swap( _states );
						return std::make_pair( std::distance( beg, cur ), Error );
					}
					break;
				case Bulk:
				{
					size_t available = std::distance( beg, end ) - std::distance( beg, cur ) + 1;
					size_t canRead = std::min( available, _bulk_size );

					_buf.insert( _buf.end(), cur - 1, cur - 1 + canRead );
					_bulk_size -= canRead;
					cur += canRead - 1;

					if ( _bulk_size == 0 )
					{
						state = BulkCR;
					}
					break;
				}
				case BulkCR:
					if ( c == '\r' )
					{
						state = BulkLF;
					}
					else
					{
						std::stack<state_t>().swap( _states );
						return std::make_pair( std::distance( beg, cur ), Error );
					}
					break;
				case BulkLF:
					if ( c == '\n' )
					{
						state = Start;
						_value = value( _buf );
					}
					else
					{
						std::stack<state_t>().swap( _states );
						return std::make_pair( std::distance( beg, cur ), Error );
					}
					break;
				case ArraySize:
					if ( c == '\r' )
					{
						if ( _buf.empty() )
						{
							std::stack<state_t>().swap( _states );
							return std::make_pair( std::distance( beg, cur ), Error );
						}
						else
						{
							state = ArraySizeLF;
						}
					}
					else if ( isdigit( c ) || c == '-' )
					{
						_buf.push_back( c );
					}
					else
					{
						std::stack<state_t>().swap( _states );
						return std::make_pair( std::distance( beg, cur ), Error );
					}
					break;
				case ArraySizeLF:
					if ( c == '\n' )
					{
						int64_t arraySize = to_long( _buf.data(), _buf.size() );
						std::vector<value> array;

						if ( arraySize == -1 )
						{
							state = Start;
							_value = value();
						}
						else if ( arraySize == 0 )
						{
							state = Start;
							_value = value( std::move( array ) );
						}
						else if ( arraySize < 0 )
						{
							std::stack<state_t>().swap( _states );
							return std::make_pair( std::distance( beg, cur ), Error );
						}
						else
						{
							array.reserve( (size_t)arraySize );

							_array_sizes.push( arraySize );
							_array_values.push( std::move( array ) );

							state = StartArray;
						}
					}
					else
					{
						std::stack<state_t>().swap( _states );
						return std::make_pair( std::distance( beg, cur ), Error );
					}
					break;
				case Integer:
					if ( c == '\r' )
					{
						if ( _buf.empty() )
						{
							std::stack<state_t>().swap( _states );
							return std::make_pair( std::distance( beg, cur ), Error );
						}
						else
						{
							state = IntegerLF;
						}
					}
					else if ( isdigit( c ) || c == '-' )
					{
						_buf.push_back( c );
					}
					else
					{
						std::stack<state_t>().swap( _states );
						return std::make_pair( std::distance( beg, cur ), Error );
					}
					break;
				case IntegerLF:
					if ( c == '\n' )
					{
						_value = value( to_long( _buf.data(), _buf.size() ) );
						_buf.clear();
						state = Start;
					}
					else
					{
						std::stack<state_t>().swap( _states );
						return std::make_pair( std::distance( beg, cur ), Error );
					}
					break;
				default:
					std::stack<state_t>().swap( _states );
					return std::make_pair( std::distance( beg, cur ), Error );
				}


				if ( state == Start )
				{
					if ( !_array_sizes.empty() )
					{
						_array_values.top().get_array().push_back( _value );

						while ( !_array_sizes.empty() && --_array_sizes.top() == 0 )
						{
							_array_sizes.pop();
							_value = std::move( _array_values.top() );
							_array_values.pop();

							if ( !_array_sizes.empty() )
								_array_values.top().get_array().push_back( _value );
						}
					}

					if ( _array_sizes.empty() )
					{
						break;
					}
				}
			}

			if ( _array_sizes.empty() && state == Start )
			{
				return std::make_pair( std::distance( beg, cur ), Completed );
			}
			else
			{
				_states.push( state );
				return std::make_pair( std::distance( beg, cur ), Incompleted );
			}
		}

	private:
		inline bool is_char( int c )
		{
			return c >= 0 && c <= 127;
		}

		inline bool is_control( int c )
		{
			return ( c >= 0 && c <= 31 ) || ( c == 127 );
		}

		long int to_long( const char * str, size_t size )
		{
			long int value = 0;
			bool sign = false;

			if ( str == nullptr || size == 0 )
			{
				return 0;
			}

			if ( *str == '-' )
			{
				sign = true;
				++str;
				--size;

				if ( size == 0 )
				{
					return 0;
				}
			}

			for ( const char * end = str + size; str != end; ++str )
			{
				char c = *str;
				value = value * 10;
				value += c - '0';
			}

			return sign ? -value : value;
		}

	private:
		std::string _buf;
		size_t _bulk_size;
		value _value;
		std::stack<state_t> _states;
		std::stack<int64_t> _array_sizes;
		std::stack<value> _array_values;
	};

	class client
	{
	public:
		using result_callback_t = std::function< void( value ) >;
		using output_callback_t = std::function< void( std::string_view ) >;

	public:
		client( output_callback_t out_cb )
			:_output( out_cb )
		{ }

		~client() = default;

	public:
		template< typename Iterator > Iterator input( Iterator beg, Iterator end )
		{
			std::unique_lock< std::mutex > lock( _rmutex );

			Iterator cur = beg;

			while ( cur != end )
			{
				auto result = _parser.parse( cur, end );

				if ( result.second == parser::Completed )
				{
					consume_message( _parser.result() );
					std::advance( cur, result.first );
				}
				else if ( result.second == parser::Incompleted )
				{
					std::advance( cur, result.first );
					break;
				}
				else
				{
					consume_message( value( 0, "redis parse error" ) );
					
					return end;
				}
			}

			return cur;
		}

	public:
		void command( const std::vector< std::string_view > & args, result_callback_t callback, std::string_view subscribe_key = {} )
		{
			std::string cmd;

			cmd.append( "*" ).append( std::to_string( args.size() ) ).append( CRCF_MATCH );

			for ( const auto & item : args )
			{
				cmd.append( "$" ).append( std::to_string( item.size() ) ).append( CRCF_MATCH );
				cmd.append( item ).append( CRCF_MATCH );
			}

			if( _output != nullptr )
			{
				std::unique_lock< std::mutex >lock( _wmutex );

				if ( subscribe_key.empty() )
				{
					_handler.emplace_back( std::move( callback ) );
				}
				else
				{
					_subscribe_handler.insert( { std::string( subscribe_key.begin(), subscribe_key.end() ), std::move( callback ) } );
				}

				_output( cmd );
			}
		}

	public:
		void ping( result_callback_t callback )
		{
			command( { "PING" }, std::move( callback ) );
		}

		void echo( std::string_view message, result_callback_t callback )
		{
			command( { "ECHO", message }, std::move( callback ) );
		}

		void auth( std::string_view password, result_callback_t callback )
		{
			command( { "AUTH", password }, std::move( callback ) );
		}

		void select( int index, result_callback_t callback )
		{
			command( { "SELECT", std::to_string( index ) }, std::move( callback ) );
		}

		void quit( result_callback_t callback )
		{
			command( { "QUIT" }, std::move( callback ) );
		}

	public:
		void set( std::string_view key, std::string_view value, result_callback_t callback )
		{
			command( { "SET", key, value }, std::move( callback ) );
		}

		void get( std::string_view key, result_callback_t callback )
		{
			command( { "GET", key }, std::move( callback ) );
		}

		void del( std::string_view key, result_callback_t callback )
		{
			command( { "DEL", key }, std::move( callback ) );
		}

	public:
		void hset( std::string_view key, std::string_view field, std::string_view value, result_callback_t callback )
		{
			command( { "HSET", key, field, value }, std::move( callback ) );
		}

		void hget( std::string_view key, std::string_view field, result_callback_t callback )
		{
			command( { "HGET", key, field }, std::move( callback ) );
		}

		void hdel( std::string_view key, std::string_view field, result_callback_t callback )
		{
			command( { "HDEL", key, field }, std::move( callback ) );
		}

	public:
		void sadd( std::string_view key, const std::vector<std::string_view> & members, result_callback_t callback )
		{
			std::vector<std::string_view> args{ "SADD", key };
			args.insert( args.end(), members.begin(), members.end() );
			command( args, std::move( callback ) );
		}

		void scard( std::string_view key, result_callback_t callback )
		{
			command( { "SCARD", key }, std::move( callback ) );
		}

		void sdiff( std::string_view key, const std::vector<std::string_view> & keys, result_callback_t callback )
		{
			std::vector<std::string_view> args{ "SDIFF", key };
			args.insert( args.end(), keys.begin(), keys.end() );
			command( args, std::move( callback ) );
		}

		void sdiffstore( std::string_view destination, std::string_view key, const std::vector<std::string_view> & keys, result_callback_t callback )
		{
			std::vector<std::string_view> args{ "SDIFFSTORE", destination, key };
			args.insert( args.end(), keys.begin(), keys.end() );
			command( args, std::move( callback ) );
		}

		void sinter( std::string_view key, const std::vector<std::string_view> & keys, result_callback_t callback )
		{
			std::vector<std::string_view> args{ "SINTER", key };
			args.insert( args.end(), keys.begin(), keys.end() );
			command( args, std::move( callback ) );
		}

		void sinterstore( std::string_view destination, std::string_view key, const std::vector<std::string_view> & keys, result_callback_t callback )
		{
			std::vector<std::string_view> args{ "SINTERSTORE", destination, key };
			args.insert( args.end(), keys.begin(), keys.end() );
			command( args, std::move( callback ) );
		}

		void sismember( std::string_view key, std::string_view member, result_callback_t callback )
		{
			command( { "SISMEMBER", key, member }, std::move( callback ) );
		}

		void smembers( std::string_view key, result_callback_t callback )
		{
			command( { "SMEMBERS", key }, std::move( callback ) );
		}

		void smove( std::string_view source, std::string_view destination, std::string_view member, result_callback_t callback )
		{
			command( { "SMOVE", source, destination, member }, std::move( callback ) );
		}

		void spop( std::string_view key, result_callback_t callback )
		{
			command( { "SPOP", key }, std::move( callback ) );
		}

		void srandmember( std::string_view key, result_callback_t callback )
		{
			command( { "SRANDMEMBER", key }, std::move( callback ) );
		}

		void srandmember( std::string_view key, int count, result_callback_t callback )
		{
			command( { "SRANDMEMBER", key, std::to_string( count ) }, std::move( callback ) );
		}

		void srem( std::string_view key, std::string_view member, const std::vector<std::string_view> & members, result_callback_t callback )
		{
			std::vector<std::string_view> args{ "SREM", key, member };
			args.insert( args.end(), members.begin(), members.end() );
			command( args, std::move( callback ) );
		}

		void sunion( std::string_view key, const std::vector<std::string_view> & keys, result_callback_t callback )
		{
			std::vector<std::string_view> args{ "SUNION", key };
			args.insert( args.end(), keys.begin(), keys.end() );
			command( args, std::move( callback ) );
		}

		void sunionstore( std::string_view destination, std::string_view key, const std::vector<std::string_view> & keys, result_callback_t callback )
		{
			std::vector<std::string_view> args{ "SUNIONSTORE", destination, key };
			args.insert( args.end(), keys.begin(), keys.end() );
			command( args, std::move( callback ) );
		}

		void sscan( std::string_view key, int cursor, std::string_view pattern, int count, result_callback_t callback )
		{
			command( { "SSCAN", key, std::to_string( cursor ), pattern, std::to_string( count ) }, std::move( callback ) );
		}

	public:
		void publish( std::string_view key, std::string_view msg, result_callback_t callback )
		{
			command( { "PUBLISH", key, msg }, std::move( callback ) );
		}

		void subscribe( std::string_view key, result_callback_t callback )
		{
			command( { "SUBSCRIBE", key }, std::move( callback ), key );
		}

		void unsubscribe( std::string_view key, result_callback_t callback )
		{
			command( { "UNSUBSCRIBE", key }, std::move( callback ) );

			auto it = _subscribe_handler.find( { key.begin(), key.end() } );
			if ( it != _subscribe_handler.end() )
			{
				_subscribe_handler.erase( it );
			}
		}

	private:
		void consume_message( const value & val )
		{
			if ( val.is_array() )
			{
				std::string_view cmd = val.get_array()[0].get_string();

				if ( cmd == "message" )
				{
					auto it = _subscribe_handler.find( val.get_array()[1].get_string() );
					if ( it != _subscribe_handler.end() )
					{
						it->second( val.get_array()[2] );
					}

					return;
				}
				else if ( cmd == "subscribe" )
				{
					return;
				}
			}

			std::unique_lock< std::mutex > lock( _wmutex );
			{
				auto handler = std::move( _handler.front() );
				_handler.pop_front();
				handler( val );
			}
		}

	private:
		parser _parser;
		output_callback_t _output;
		std::mutex _rmutex, _wmutex;
		std::deque<result_callback_t> _handler;
		std::map<std::string, result_callback_t> _subscribe_handler;
	};
}

#endif//XREDIS_HPP__94D2E943_814E_4967_A639_26765ED2C208
