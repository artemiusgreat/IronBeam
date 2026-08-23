using Flurl;
using Flurl.Http;
using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Runtime.Serialization;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;

namespace IronBeam
{
  public partial class IronBeamBroker
  {
    public string BaseUrl { get; set; } = "https://live.ironbeamapi.com/v2";
    public static JsonSerializerOptions Options { get; } = new JsonSerializerOptions
    {
      PropertyNameCaseInsensitive = true,
      DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
      Converters = { new JsonStringEnumConverter() }
    };

    public string AccessToken { get; set; }

    #region Core Send Method
    /// <summary>
    /// Send data to the API using Flurl.Http
    /// </summary>
    public virtual async Task<T> Send<T>(SenderQuery query)
    {
      var message = $"{new UriBuilder(query.Source)}"
          .WithHeader("Accept", "application/json")
          .WithHeader("Authorization", $"Bearer {AccessToken}");

      var data = null as StringContent;

      if (query.Content is not null)
      {
        data = new StringContent(JsonSerializer.Serialize(query.Content, Options), Encoding.UTF8, "application/json");
      }

      var response = await message
          .SendAsync(query.Action ?? HttpMethod.Get, data, HttpCompletionOption.ResponseContentRead, query.Cleaner ?? CancellationToken.None)
          .ConfigureAwait(false);

      if (query.ResponseHeaders != null && query.Headers != null)
      {
        foreach (var key in query.Headers.Keys)
        {
          if (response.ResponseMessage.Headers.TryGetValues(key, out var values))
          {
            query.ResponseHeaders[key] = values;
          }
        }
      }

      var responseContent = await response
          .ResponseMessage
          .Content
          .ReadAsStringAsync()
          .ConfigureAwait(false);

      return JsonSerializer.Deserialize<T>(responseContent, Options);
    }
    #endregion

    #region API Methods
    /// <summary>Authorize to use the API.</summary>
    public virtual async Task<AuthorizationResponse> AuthorizeAsync(AuthorizationRequest body, CancellationToken cancellationToken = default)
    {
      return await Send<AuthorizationResponse>(new()
      {
        Source = BaseUrl.AppendPathSegment("auth"),
        Action = HttpMethod.Post,
        Content = body,
        Cleaner = cancellationToken
      });
    }

    /// <summary>Logout user and invalidate token.</summary>
    public virtual async Task<SuccessResponse> LogoutAsync(CancellationToken cancellationToken = default)
    {
      return await Send<SuccessResponse>(new()
      {
        Source = BaseUrl.AppendPathSegment("logout"),
        Action = HttpMethod.Post,
        Content = new { },
        Cleaner = cancellationToken
      });
    }

    /// <summary>Get trader information by Trader ID.</summary>
    public virtual async Task<TraderInfoResponse> GetTraderInfoAsync(string traderId, CancellationToken cancellationToken = default)
    {
      return await Send<TraderInfoResponse>(new()
      {
        Source = BaseUrl.AppendPathSegment("info/trader").SetQueryParam("traderId", traderId),
        Action = HttpMethod.Get,
        Cleaner = cancellationToken
      });
    }

    /// <summary>Place a new order to the exchange.</summary>
    public virtual async Task<OrderBaseResponse> PlaceOrderAsync(string accountId, OrderRequest body, CancellationToken cancellationToken = default)
    {
      return await Send<OrderBaseResponse>(new()
      {
        Source = BaseUrl.AppendPathSegment($"order/{accountId}/place"),
        Action = HttpMethod.Post,
        Content = body,
        Cleaner = cancellationToken
      });
    }

    /// <summary>Get security definitions.</summary>
    public virtual async Task<SecurityDefinitionsResponse> GetSecurityDefinitionsAsync(IEnumerable<string> symbols, CancellationToken cancellationToken = default)
    {
      return await Send<SecurityDefinitionsResponse>(new()
      {
        Source = BaseUrl.AppendPathSegment("info/security/definitions").SetQueryParam("symbols", symbols),
        Action = HttpMethod.Get,
        Cleaner = cancellationToken
      });
    }

    /// <summary>Get symbols.</summary>
    public virtual async Task<SymbolsResponse> GetSymbolsAsync(string text = null, int? limit = null, bool? preferActive = null, CancellationToken cancellationToken = default)
    {
      return await Send<SymbolsResponse>(new()
      {
        Source = BaseUrl.AppendPathSegment("info/symbols").SetQueryParams(new { text, limit, preferActive }),
        Action = HttpMethod.Get,
        Cleaner = cancellationToken
      });
    }

    /// <summary>Get account balance.</summary>
    public virtual async Task<AccountBalanceResponse> AccountBalanceAsync(string accountId, BalanceType balanceType, CancellationToken cancellationToken = default)
    {
      return await Send<AccountBalanceResponse>(new()
      {
        Source = BaseUrl.AppendPathSegment($"account/{accountId}/balance").SetQueryParam("balanceType", balanceType),
        Action = HttpMethod.Get,
        Cleaner = cancellationToken
      });
    }

    /// <summary>Get positions.</summary>
    public virtual async Task<PositionsResponse> PositionsAsync(string accountId, CancellationToken cancellationToken = default)
    {
      return await Send<PositionsResponse>(new()
      {
        Source = BaseUrl.AppendPathSegment($"account/{accountId}/positions"),
        Action = HttpMethod.Get,
        Cleaner = cancellationToken
      });
    }

    /// <summary>Get orders.</summary>
    public virtual async Task<OrdersResponse> GetOrdersAsync(string accountId, OrderStatusType orderStatus, CancellationToken cancellationToken = default)
    {
      return await Send<OrdersResponse>(new()
      {
        Source = BaseUrl.AppendPathSegment($"order/{accountId}/{orderStatus}"),
        Action = HttpMethod.Get,
        Cleaner = cancellationToken
      });
    }

    /// <summary>Cancel order.</summary>
    public virtual async Task<OrdersResponse> CancelOrderAsync(string accountId, string orderId, CancellationToken cancellationToken = default)
    {
      return await Send<OrdersResponse>(new()
      {
        Source = BaseUrl.AppendPathSegment($"order/{accountId}/cancel/{orderId}"),
        Action = HttpMethod.Delete,
        Cleaner = cancellationToken
      });
    }
    #endregion
  }

  #region Supporting Types
  public class SenderQuery
  {
    public string Source { get; set; }
    public HttpMethod Action { get; set; } = HttpMethod.Get;
    public object Content { get; set; }
    public CancellationToken? Cleaner { get; set; }
    public Dictionary<string, string> Headers { get; set; }
    public Dictionary<string, IEnumerable<string>> ResponseHeaders { get; set; } = new();
  }
  #endregion

  #region DTOs & Enums (Cleaned of validation attributes)
  public partial class AuthorizationRequest
  {
    [JsonPropertyName("username")] public string Username { get; set; }
    [JsonPropertyName("password")] public string Password { get; set; }
    [JsonPropertyName("apikey")] public string Apikey { get; set; }
  }

  public enum ResponseStatus
  {
    [EnumMember(Value = "OK")] OK = 0,
    [EnumMember(Value = "ERROR")] ERROR = 1,
    [EnumMember(Value = "WARNING")] WARNING = 2,
    [EnumMember(Value = "INFO")] INFO = 3,
    [EnumMember(Value = "FATAL")] FATAL = 4,
    [EnumMember(Value = "UNKNOWN")] UNKNOWN = 5,
  }

  public partial class Response
  {
    [JsonPropertyName("status")] public ResponseStatus Status { get; set; }
    [JsonPropertyName("message")] public string Message { get; set; }
  }

  public partial class AuthorizationResponse : Response
  {
    [JsonPropertyName("token")] public string Token { get; set; }
  }

  public partial class Error : Response
  {
    [JsonPropertyName("error")] public string Error1 { get; set; }
  }

  public partial class SuccessResponse : Response
  {
    [JsonExtensionData] public Dictionary<string, object> AdditionalProperties { get; set; } = new();
  }

  public partial class TraderInfoResponse : Response
  {
    [JsonPropertyName("accounts")] public List<string> Accounts { get; set; }
    [JsonPropertyName("isLive")] public bool IsLive { get; set; }
    [JsonPropertyName("traderId")] public string TraderId { get; set; }
    [JsonExtensionData] public Dictionary<string, object> AdditionalProperties { get; set; } = new();
  }

  public partial class OrderRequest
  {
    [JsonPropertyName("accountId")] public string AccountId { get; set; }
    [JsonPropertyName("exchSym")] public string ExchSym { get; set; }
    [JsonPropertyName("side")] public OrderSide Side { get; set; }
    [JsonPropertyName("quantity")] public double Quantity { get; set; }
    [JsonPropertyName("limitPrice")] public double LimitPrice { get; set; }
    [JsonPropertyName("stopPrice")] public double StopPrice { get; set; }
    [JsonPropertyName("stopLoss")] public double StopLoss { get; set; }
    [JsonPropertyName("takeProfit")] public double TakeProfit { get; set; }
    [JsonPropertyName("stopLossOffset")] public float StopLossOffset { get; set; }
    [JsonPropertyName("takeProfitOffset")] public float TakeProfitOffset { get; set; }
    [JsonPropertyName("trailingStop")] public float TrailingStop { get; set; }
    [JsonPropertyName("orderType")] public OrderType OrderType { get; set; }
    [JsonPropertyName("duration")] public DurationType Duration { get; set; }
    [JsonPropertyName("waitForOrderId")] public bool WaitForOrderId { get; set; } = true;
  }

  public partial class OrderBaseResponse : SuccessResponse
  {
    [JsonPropertyName("orderId")] public string OrderId { get; set; }
    [JsonPropertyName("strategyId")] public long StrategyId { get; set; }
  }

  public enum OrderSide
  {
    [EnumMember(Value = "BUY")] BUY = 0,
    [EnumMember(Value = "SELL")] SELL = 1,
    [EnumMember(Value = "INVALID")] INVALID = 2,
  }

  public enum OrderType
  {
    [EnumMember(Value = "")] INVALID = 0,
    [EnumMember(Value = "1")] MARKET = 1,
    [EnumMember(Value = "2")] LIMIT = 2,
    [EnumMember(Value = "3")] STOP = 3,
    [EnumMember(Value = "4")] STOP_LIMIT = 4,
  }

  public enum DurationType
  {
    [EnumMember(Value = "")] INVALID = 0,
    [EnumMember(Value = "0")] DAY = 1,
    [EnumMember(Value = "1")] GOOD_TILL_CANCEL = 2,
  }

  public enum BalanceType
  {
    [EnumMember(Value = "CURRENT_OPEN")] CURRENT_OPEN = 0,
    [EnumMember(Value = "START_OF_DAY")] START_OF_DAY = 1,
  }

  public enum OrderStatusType
  {
    [EnumMember(Value = "ANY ")] ANY = 0,
    [EnumMember(Value = "INVALID ")] INVALID = 1,
    [EnumMember(Value = "SUBMITTED ")] SUBMITTED = 2,
    [EnumMember(Value = "NEW ")] NEW = 3,
    [EnumMember(Value = "PARTIALLY_FILLED ")] PARTIALLY_FILLED = 4,
    [EnumMember(Value = "FILLED ")] FILLED = 5,
    [EnumMember(Value = "CANCELLED ")] CANCELLED = 7,
    [EnumMember(Value = "REJECTED ")] REJECTED = 11,
    [EnumMember(Value = "EXPIRED ")] EXPIRED = 15,
  }

  public partial class SymbolsResponse : Response
  {
    [JsonPropertyName("symbols")] public List<SymbolInfo> Symbols { get; set; }
    [JsonExtensionData] public Dictionary<string, object> AdditionalProperties { get; set; } = new();
  }

  public partial class SymbolInfo
  {
    [JsonPropertyName("symbol")] public string Symbol { get; set; }
    [JsonPropertyName("currency")] public string Currency { get; set; }
    [JsonPropertyName("description")] public string Description { get; set; }
    [JsonPropertyName("hasQuotes")] public bool HasQuotes { get; set; }
  }

  public partial class SecurityDefinitionsResponse : Response
  {
    [JsonPropertyName("securityDefinitions")] public List<SecurityDefinition> SecurityDefinitions { get; set; }
  }

  public partial class SecurityDefinition
  {
    [JsonPropertyName("exchSym")] public string ExchSym { get; set; }
    [JsonPropertyName("securityType")] public SecurityType SecurityType { get; set; }
  }

  public enum SecurityType
  {
    [EnumMember(Value = "INVALID")] INVALID = 0,
    [EnumMember(Value = "FUT")] FUT = 1,
    [EnumMember(Value = "OPT")] OPT = 2,
  }

  public partial class AccountBalanceResponse : Response
  {
    [JsonPropertyName("balances")] public List<Balance> Balances { get; set; }
    [JsonExtensionData] public Dictionary<string, object> AdditionalProperties { get; set; } = new();
  }

  public partial class Balance
  {
    [JsonPropertyName("accountId")] public string AccountId { get; set; }
    [JsonPropertyName("currencyCode")] public string CurrencyCode { get; set; }
    [JsonPropertyName("netLiquidity")] public double NetLiquidity { get; set; }
    [JsonPropertyName("balanceType")] public BalanceType BalanceType { get; set; }
  }

  public partial class PositionsResponse : Response
  {
    [JsonPropertyName("accountId")] public string AccountId { get; set; }
    [JsonPropertyName("positions")] public List<Position> Positions { get; set; }
    [JsonExtensionData] public Dictionary<string, object> AdditionalProperties { get; set; } = new();
  }

  public partial class Position
  {
    [JsonPropertyName("accountId")] public string AccountId { get; set; }
    [JsonPropertyName("exchSym")] public string ExchSym { get; set; }
    [JsonPropertyName("quantity")] public double Quantity { get; set; }
    [JsonPropertyName("price")] public double Price { get; set; }
    [JsonPropertyName("side")] public PositionSide Side { get; set; }
  }

  public enum PositionSide
  {
    [EnumMember(Value = "LONG")] LONG = 0,
    [EnumMember(Value = "SHORT")] SHORT = 1,
  }

  public partial class OrdersResponse : Response
  {
    [JsonPropertyName("orders")] public List<Order> Orders { get; set; }
    [JsonExtensionData] public Dictionary<string, object> AdditionalProperties { get; set; } = new();
  }

  public partial class Order
  {
    [JsonPropertyName("orderId")] public string OrderId { get; set; }
    [JsonPropertyName("accountId")] public string AccountId { get; set; }
    [JsonPropertyName("exchSym")] public string ExchSym { get; set; }
    [JsonPropertyName("status")] public OrderStatusType Status { get; set; }
    [JsonPropertyName("side")] public OrderSide Side { get; set; }
    [JsonPropertyName("quantity")] public double Quantity { get; set; }
  }
  #endregion
}