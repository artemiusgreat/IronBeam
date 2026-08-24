using Flurl;
using Flurl.Http;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Net.WebSockets;
using System.Runtime.Serialization;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using System.Timers;

namespace IronBeam
{
  public partial class IronBeamBroker : IDisposable
  {
    /// <summary>
    /// Web socket for events
    /// </summary>
    protected ClientWebSocket dataStreamer;

    /// <summary>
    /// Web socket for account
    /// </summary>
    protected ClientWebSocket accountStreamer;

    /// <summary>
    /// Disposable connections
    /// </summary>
    protected IList<IDisposable> connections = [];

    /// <summary>
    /// Token
    /// </summary>
    public string AccessToken { get; set; }

    /// <summary>
    /// Base URI
    /// </summary>
    public string DataUri { get; set; }

    /// <summary>
    /// Socket endpoint
    /// </summary>
    public string StreamUri { get; set; }

    /// <summary>
    /// Price notification
    /// </summary>
    public virtual Action<QuoteFull> OnPrice { get; set; } = o => { };

    /// <summary>
    /// Order notification
    /// </summary>
    public virtual Action<OrderBaseResponse> OnOrder { get; set; } = o => { };

    /// <summary>
    /// Error notification
    /// </summary>
    public virtual Action<Exception> OnError { get; set; } = o => { };

    /// <summary>
    /// Serialization options for JSON
    /// </summary>
    public static JsonSerializerOptions Options { get; } = new JsonSerializerOptions
    {
      PropertyNameCaseInsensitive = true,
      DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
      Converters = { new JsonStringEnumConverter() }
    };

    /// <summary>
    /// Constructor
    /// </summary>
    public IronBeamBroker()
    {
      DataUri = "https://live.ironbeamapi.com/v2";
      StreamUri = "wss://live.ironbeamapi.com/v2/stream";
    }

    /// <summary>
    /// Dispose
    /// </summary>
    public virtual void Dispose()
    {
      Disconnect();
    }

    /// <summary>
    /// Connect
    /// </summary>
    /// <param name="streamId"></param>
    /// <param name="cts"></param>
    public virtual async Task Connect(string streamId, CancellationToken cts = default)
    {
      dataStreamer = new ClientWebSocket();

      await CreateConnection($"/stream/{streamId}", dataStreamer, message =>
      {
        var messageType = $"{message["type"]}";

        switch (messageType)
        {
          case "q": OnPrice(message.Deserialize<QuoteFull>(Options)); break;
          case "tb": break;
          case "tc": break;
          case "ti": break;
          case "vb": break;
          case "ps": break;
          case "psa": break;
          case "f": break;
          case "ri": break;
          case "ria": break;
          case "o": break;
          case "d": break;
          case "tr": break;
          case "b": break;
          case "ba": break;
          case "i": break;
          case "r": break;
        }
      });

      connections.Add(dataStreamer);
    }

    /// <summary>
    /// Save state and dispose
    /// </summary>
    public virtual void Disconnect()
    {
      OnPrice = o => { };
      OnOrder = o => { };
      connections?.ForEach(o => o?.Dispose());
      connections?.Clear();
    }

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
            query.ResponseHeaders[key] = [.. values];
          }
        }
      }

      var responseContent = await response.ResponseMessage.Content.ReadAsStringAsync().ConfigureAwait(false);
      return JsonSerializer.Deserialize<T>(responseContent, Options);
    }

    #endregion

    #region API Methods

    /// <summary>Authorize to use the API.</summary>
    public virtual async Task<AuthorizationResponse> AuthorizeAsync(AuthorizationRequest body, CancellationToken cts = default)
    {
      return await Send<AuthorizationResponse>(new()
      {
        Source = DataUri.AppendPathSegment("auth"),
        Action = HttpMethod.Post,
        Content = body,
        Cleaner = cts
      });
    }

    /// <summary>Logout user and invalidate token.</summary>
    public virtual async Task<SuccessResponse> LogoutAsync(CancellationToken cts = default)
    {
      return await Send<SuccessResponse>(new()
      {
        Source = DataUri.AppendPathSegment("logout"),
        Action = HttpMethod.Post,
        Content = new { },
        Cleaner = cts
      });
    }

    /// <summary>Get trader information by Trader ID.</summary>
    public virtual async Task<TraderInfoResponse> GetTraderInfoAsync(string traderId, CancellationToken cts = default)
    {
      return await Send<TraderInfoResponse>(new()
      {
        Source = DataUri.AppendPathSegment("info/trader").SetQueryParam("traderId", traderId),
        Action = HttpMethod.Get,
        Cleaner = cts
      });
    }

    /// <summary>Get user general info.</summary>
    public virtual async Task<UserInfoResponse> GetUserGeneralInfoAsync(string traderId, CancellationToken cts = default)
    {
      return await Send<UserInfoResponse>(new()
      {
        Source = DataUri.AppendPathSegment("info/user").SetQueryParam("traderId", traderId),
        Action = HttpMethod.Get,
        Cleaner = cts
      });
    }

    /// <summary>Get security definitions.</summary>
    public virtual async Task<SecurityDefinitionsResponse> GetSecurityDefinitionsAsync(IEnumerable<string> symbols, CancellationToken cts = default)
    {
      return await Send<SecurityDefinitionsResponse>(new()
      {
        Source = DataUri.AppendPathSegment("info/security/definitions").SetQueryParam("symbols", symbols),
        Action = HttpMethod.Get,
        Cleaner = cts
      });
    }

    /// <summary>Get security margin and value.</summary>
    public virtual async Task<SecurityMarginAndValueResponse> GetSecurityMarginAndValueAsync(IEnumerable<string> symbols, CancellationToken cts = default)
    {
      return await Send<SecurityMarginAndValueResponse>(new()
      {
        Source = DataUri.AppendPathSegment("info/security/margin").SetQueryParam("symbols", symbols),
        Action = HttpMethod.Get,
        Cleaner = cts
      });
    }

    /// <summary>Get security status.</summary>
    public virtual async Task<SecurityStatusResponse> GetSecurityStatusAsync(IEnumerable<string> symbols, CancellationToken cts = default)
    {
      return await Send<SecurityStatusResponse>(new()
      {
        Source = DataUri.AppendPathSegment("info/security/status").SetQueryParam("symbols", symbols),
        Action = HttpMethod.Get,
        Cleaner = cts
      });
    }

    /// <summary>Get symbols.</summary>
    public virtual async Task<SymbolsResponse> GetSymbolsAsync(string text = null, int? limit = null, bool? preferActive = null, CancellationToken cts = default)
    {
      return await Send<SymbolsResponse>(new()
      {
        Source = DataUri.AppendPathSegment("info/symbols").SetQueryParams(new { text, limit, preferActive }),
        Action = HttpMethod.Get,
        Cleaner = cts
      });
    }

    /// <summary>Get exchange sources.</summary>
    public virtual async Task<ExchangeSourcesResponse> GetExchangeSourcesAsync(CancellationToken cts = default)
    {
      return await Send<ExchangeSourcesResponse>(new()
      {
        Source = DataUri.AppendPathSegment("info/exchangeSources"),
        Action = HttpMethod.Get,
        Cleaner = cts
      });
    }

    /// <summary>Get market complexes.</summary>
    public virtual async Task<ComplexesResponse> GetComplexesAsync(string exchange, CancellationToken cts = default)
    {
      return await Send<ComplexesResponse>(new()
      {
        Source = DataUri.AppendPathSegment($"info/complexes/{exchange}"),
        Action = HttpMethod.Get,
        Cleaner = cts
      });
    }

    /// <summary>Get symbol futures.</summary>
    public virtual async Task<SymbolFuturesResponse> GetSymbolFuturesAsync(string exchange, string marketGroup, CancellationToken cts = default)
    {
      return await Send<SymbolFuturesResponse>(new()
      {
        Source = DataUri.AppendPathSegment($"info/symbol/search/futures/{exchange}/{marketGroup}"),
        Action = HttpMethod.Get,
        Cleaner = cts
      });
    }

    /// <summary>Get symbol groups by complex.</summary>
    public virtual async Task<ComplexGroupsResponse> GetSymbolGroupsByComplexAsync(string complex, CancellationToken cts = default)
    {
      return await Send<ComplexGroupsResponse>(new()
      {
        Source = DataUri.AppendPathSegment($"info/symbol/search/groups/{complex}"),
        Action = HttpMethod.Get,
        Cleaner = cts
      });
    }

    /// <summary>Get symbol option groups.</summary>
    public virtual async Task<SymbolOptionsResponse> GetSymbolOptionGroupsAsync(string symbol, CancellationToken cts = default)
    {
      return await Send<SymbolOptionsResponse>(new()
      {
        Source = DataUri.AppendPathSegment($"info/symbol/search/options/{symbol}"),
        Action = HttpMethod.Get,
        Cleaner = cts
      });
    }

    /// <summary>Get search symbol option.</summary>
    public virtual async Task<SymbolSearchOptionsResponse> GetSearchSymbolOptionAsync(string symbol, string group, OptionType optionType, bool near, CancellationToken cts = default)
    {
      return await Send<SymbolSearchOptionsResponse>(new()
      {
        Source = DataUri.AppendPathSegment($"info/symbol/search/options/ext/{symbol}/{group}/{optionType}/{near}"),
        Action = HttpMethod.Get,
        Cleaner = cts
      });
    }

    /// <summary>Get search for option spreads.</summary>
    public virtual async Task<SymbolOptionSpreadsResponse> GetSearchForOptionSpreadsAsync(string symbol, CancellationToken cts = default)
    {
      return await Send<SymbolOptionSpreadsResponse>(new()
      {
        Source = DataUri.AppendPathSegment($"info/symbol/search/options/spreads/{symbol}"),
        Action = HttpMethod.Get,
        Cleaner = cts
      });
    }

    /// <summary>Get strategy ID.</summary>
    public virtual async Task<StrategyIdResponse> GetStrategyIdAsync(CancellationToken cts = default)
    {
      return await Send<StrategyIdResponse>(new()
      {
        Source = DataUri.AppendPathSegment("info/strategyId"),
        Action = HttpMethod.Get,
        Cleaner = cts
      });
    }

    /// <summary>Get account balance.</summary>
    public virtual async Task<AccountBalanceResponse> AccountBalanceAsync(string accountId, BalanceType balanceType, CancellationToken cts = default)
    {
      return await Send<AccountBalanceResponse>(new()
      {
        Source = DataUri.AppendPathSegment($"account/{accountId}/balance").SetQueryParam("balanceType", balanceType),
        Action = HttpMethod.Get,
        Cleaner = cts
      });
    }

    /// <summary>Get positions.</summary>
    public virtual async Task<PositionsResponse> PositionsAsync(string accountId, CancellationToken cts = default)
    {
      return await Send<PositionsResponse>(new()
      {
        Source = DataUri.AppendPathSegment($"account/{accountId}/positions"),
        Action = HttpMethod.Get,
        Cleaner = cts
      });
    }

    /// <summary>Get account risk.</summary>
    public virtual async Task<AccountRiskResponse> RiskAsync(string accountId, CancellationToken cts = default)
    {
      return await Send<AccountRiskResponse>(new()
      {
        Source = DataUri.AppendPathSegment($"account/{accountId}/risk"),
        Action = HttpMethod.Get,
        Cleaner = cts
      });
    }

    /// <summary>Get account fills.</summary>
    public virtual async Task<AccountFillsResponse> FillsAsync(string accountId, CancellationToken cts = default)
    {
      return await Send<AccountFillsResponse>(new()
      {
        Source = DataUri.AppendPathSegment($"account/{accountId}/fills"),
        Action = HttpMethod.Get,
        Cleaner = cts
      });
    }

    /// <summary>Get all accounts.</summary>
    public virtual async Task<AllAccountsResponse> AllAccountsAsync(CancellationToken cts = default)
    {
      return await Send<AllAccountsResponse>(new()
      {
        Source = DataUri.AppendPathSegment("account/getAllAccounts"),
        Action = HttpMethod.Get,
        Cleaner = cts
      });
    }

    /// <summary>Get all accounts balance.</summary>
    public virtual async Task<AccountBalanceResponse> AllAccountsBalanceAsync(BalanceType balanceType, CancellationToken cts = default)
    {
      return await Send<AccountBalanceResponse>(new()
      {
        Source = DataUri.AppendPathSegment("account/getAllBalances").SetQueryParam("balanceType", balanceType),
        Action = HttpMethod.Get,
        Cleaner = cts
      });
    }

    /// <summary>Get all accounts fills.</summary>
    public virtual async Task<AccountFillsResponse> AllfillsAsync(CancellationToken cts = default)
    {
      return await Send<AccountFillsResponse>(new()
      {
        Source = DataUri.AppendPathSegment("account/getAllFills"),
        Action = HttpMethod.Get,
        Cleaner = cts
      });
    }

    /// <summary>Get all accounts positions.</summary>
    public virtual async Task<AccountsPositionsResponse> AllpositionsAsync(CancellationToken cts = default)
    {
      return await Send<AccountsPositionsResponse>(new()
      {
        Source = DataUri.AppendPathSegment("account/getAllPositions"),
        Action = HttpMethod.Get,
        Cleaner = cts
      });
    }

    /// <summary>Get all accounts risk info.</summary>
    public virtual async Task<AccountRiskResponse> AllaccountsriskinfoAsync(CancellationToken cts = default)
    {
      return await Send<AccountRiskResponse>(new()
      {
        Source = DataUri.AppendPathSegment("account/getAllRiskInfo"),
        Action = HttpMethod.Get,
        Cleaner = cts
      });
    }

    /// <summary>Get quotes.</summary>
    public virtual async Task<QuotesResponse> GetQuotesAsync(IEnumerable<string> symbols, CancellationToken cts = default)
    {
      return await Send<QuotesResponse>(new()
      {
        Source = DataUri.AppendPathSegment("market/quotes").SetQueryParam("symbols", symbols),
        Action = HttpMethod.Get,
        Cleaner = cts
      });
    }

    /// <summary>Get depth.</summary>
    public virtual async Task<DepthResponse> GetDepthAsync(IEnumerable<string> symbols, CancellationToken cts = default)
    {
      return await Send<DepthResponse>(new()
      {
        Source = DataUri.AppendPathSegment("market/depth").SetQueryParam("symbols", symbols),
        Action = HttpMethod.Get,
        Cleaner = cts
      });
    }

    /// <summary>Get trades.</summary>
    public virtual async Task<TradesResponse> GetTradesAsync(string symbol, long from, long to, int max, bool earlier, CancellationToken cts = default)
    {
      return await Send<TradesResponse>(new()
      {
        Source = DataUri.AppendPathSegment($"market/trades/{symbol}/{from}/{to}/{max}/{earlier}"),
        Action = HttpMethod.Get,
        Cleaner = cts
      });
    }

    /// <summary>Place a new order to the exchange.</summary>
    public virtual async Task<OrderBaseResponse> PlaceOrderAsync(string accountId, OrderRequest body, CancellationToken cts = default)
    {
      return await Send<OrderBaseResponse>(new()
      {
        Source = DataUri.AppendPathSegment($"order/{accountId}/place"),
        Action = HttpMethod.Post,
        Content = body,
        Cleaner = cts
      });
    }

    /// <summary>Get orders.</summary>
    public virtual async Task<OrdersResponse> GetOrdersAsync(string accountId, OrderStatusType orderStatus, CancellationToken cts = default)
    {
      return await Send<OrdersResponse>(new()
      {
        Source = DataUri.AppendPathSegment($"order/{accountId}/{orderStatus}"),
        Action = HttpMethod.Get,
        Cleaner = cts
      });
    }

    /// <summary>Get order ID from strategy ID.</summary>
    public virtual async Task<OrderBaseResponse> GetToOrderIdAsync(string accountId, long strategyId, CancellationToken cts = default)
    {
      return await Send<OrderBaseResponse>(new()
      {
        Source = DataUri.AppendPathSegment($"order/{accountId}/toorderid/{strategyId}"),
        Action = HttpMethod.Get,
        Cleaner = cts
      });
    }

    /// <summary>Get strategy ID from order ID.</summary>
    public virtual async Task<OrderBaseResponse> GetToStrategyIdAsync(string accountId, string orderId, CancellationToken cts = default)
    {
      return await Send<OrderBaseResponse>(new()
      {
        Source = DataUri.AppendPathSegment($"order/{accountId}/tostrategyId/{orderId}"),
        Action = HttpMethod.Get,
        Cleaner = cts
      });
    }

    /// <summary>Update order.</summary>
    public virtual async Task<OrdersResponse> UpdateOrderAsync(string accountId, string orderId, OrderUpdateRequest body, CancellationToken cts = default)
    {
      return await Send<OrdersResponse>(new()
      {
        Source = DataUri.AppendPathSegment($"order/{accountId}/update/{orderId}"),
        Action = HttpMethod.Put,
        Content = body,
        Cleaner = cts
      });
    }

    /// <summary>Cancel order.</summary>
    public virtual async Task<OrdersResponse> CancelOrderAsync(string accountId, string orderId, CancellationToken cts = default)
    {
      return await Send<OrdersResponse>(new()
      {
        Source = DataUri.AppendPathSegment($"order/{accountId}/cancel/{orderId}"),
        Action = HttpMethod.Delete,
        Cleaner = cts
      });
    }

    /// <summary>Cancel multiple orders.</summary>
    public virtual async Task<OrdersResponse> CancelMultipleOrdersAsync(string accountId, OrderCancelMultilpleRequest body, CancellationToken cts = default)
    {
      return await Send<OrdersResponse>(new()
      {
        Source = DataUri.AppendPathSegment($"order/{accountId}/cancelMultiple"),
        Action = HttpMethod.Delete,
        Content = body,
        Cleaner = cts
      });
    }

    /// <summary>Get orders fills.</summary>
    public virtual async Task<OrdersFillsResponse> GetOrdersFillsAsync(string accountId, CancellationToken cts = default)
    {
      return await Send<OrdersFillsResponse>(new()
      {
        Source = DataUri.AppendPathSegment($"order/{accountId}/fills"),
        Action = HttpMethod.Get,
        Cleaner = cts
      });
    }

    /// <summary>Create simulated trader.</summary>
    public virtual async Task<SimulatedTraderCreateResponse> SimulatedTraderCreateAsync(SimulatedTraderCreate body, CancellationToken cts = default)
    {
      return await Send<SimulatedTraderCreateResponse>(new()
      {
        Source = DataUri.AppendPathSegment("simulatedTraderCreate"),
        Action = HttpMethod.Post,
        Content = body,
        Cleaner = cts
      });
    }

    /// <summary>Create simulated account.</summary>
    public virtual async Task<SimulatedTraderAddAccountResponse> SimulatedAccountAddAsync(SimulatedTraderAddAccount body, CancellationToken cts = default)
    {
      return await Send<SimulatedTraderAddAccountResponse>(new()
      {
        Source = DataUri.AppendPathSegment("simulatedAccountAdd"),
        Action = HttpMethod.Post,
        Content = body,
        Cleaner = cts
      });
    }

    /// <summary>Simulated account reset.</summary>
    public virtual async Task<Response> SimulatedAccountResetAsync(SimulatedAccountReset body, CancellationToken cts = default)
    {
      return await Send<Response>(new()
      {
        Source = DataUri.AppendPathSegment("simulatedAccountReset"),
        Action = HttpMethod.Put,
        Content = body,
        Cleaner = cts
      });
    }

    /// <summary>Simulated account expire.</summary>
    public virtual async Task<Response> SimulatedAccountExpireAsync(SimulatedAccountExpire body, CancellationToken cts = default)
    {
      return await Send<Response>(new()
      {
        Source = DataUri.AppendPathSegment("simulatedAccountExpire"),
        Action = HttpMethod.Delete,
        Content = body,
        Cleaner = cts
      });
    }

    /// <summary>Simulated account add cash.</summary>
    public virtual async Task<Response> SimulatedAccountAddCashAsync(SimulatedAccountAddCash body, CancellationToken cts = default)
    {
      return await Send<Response>(new()
      {
        Source = DataUri.AppendPathSegment("simulatedAccount/addCash"),
        Action = HttpMethod.Post,
        Content = body,
        Cleaner = cts
      });
    }

    /// <summary>Simulated account get cash report.</summary>
    public virtual async Task<SimulatedAccountCashReport> SimulatedAccountGetCashReportAsync(string accountId, long startDate, long endDate, CancellationToken cts = default)
    {
      return await Send<SimulatedAccountCashReport>(new()
      {
        Source = DataUri
          .AppendPathSegment($"simulatedAccount/getCashReport/{accountId}")
          .SetQueryParam("startDate", startDate)
          .SetQueryParam("endDate", endDate),
        Action = HttpMethod.Get,
        Cleaner = cts
      });
    }

    /// <summary>Simulated account set liquidate only.</summary>
    [Obsolete]
    public virtual async Task<Response> SimulatedAccountSetLiquidateOnlyAsync(SimulatedAccountSetLiquidateOnly body, CancellationToken cts = default)
    {
      return await Send<Response>(new()
      {
        Source = DataUri.AppendPathSegment("simulatedAccount/setLiquidateOnly"),
        Action = HttpMethod.Post,
        Content = body,
        Cleaner = cts
      });
    }

    /// <summary>Simulated account liquidate.</summary>
    public virtual async Task<Response> SimulatedAccountLiquidateAsync(SimulatedAccountRiskLiquidate body, CancellationToken cts = default)
    {
      return await Send<Response>(new()
      {
        Source = DataUri.AppendPathSegment("simulatedAccount/liquidate"),
        Action = HttpMethod.Post,
        Content = body,
        Cleaner = cts
      });
    }

    /// <summary>Simulated account set risk parameters.</summary>
    public virtual async Task<Response> SimulatedAccountSetRiskAsync(SimulatedAccountSetRisk body, CancellationToken cts = default)
    {
      return await Send<Response>(new()
      {
        Source = DataUri.AppendPathSegment("simulatedAccount/setRisk"),
        Action = HttpMethod.Post,
        Content = body,
        Cleaner = cts
      });
    }

    /// <summary>Get stream ID.</summary>
    public virtual async Task<StreamIdResponse> GetStreamIdAsync(CancellationToken cts = default)
    {
      return await Send<StreamIdResponse>(new()
      {
        Source = DataUri.AppendPathSegment("stream/create"),
        Action = HttpMethod.Get,
        Cleaner = cts
      });
    }

    /// <summary>Subscribe to quotes.</summary>
    public virtual async Task<SuccessResponse> SubscribeQuotesAsync(Guid streamId, IEnumerable<string> symbols, CancellationToken cts = default)
    {
      return await Send<SuccessResponse>(new()
      {
        Source = DataUri.AppendPathSegment($"market/quotes/subscribe/{streamId}").SetQueryParam("symbols", symbols),
        Action = HttpMethod.Get,
        Cleaner = cts
      });
    }

    /// <summary>Subscribe to depths.</summary>
    public virtual async Task<SuccessResponse> SubscribeDepthsAsync(Guid streamId, IEnumerable<string> symbols, CancellationToken cts = default)
    {
      return await Send<SuccessResponse>(new()
      {
        Source = DataUri.AppendPathSegment($"market/depths/subscribe/{streamId}").SetQueryParam("symbols", symbols),
        Action = HttpMethod.Get,
        Cleaner = cts
      });
    }

    /// <summary>Subscribe to trades.</summary>
    public virtual async Task<SuccessResponse> SubscribeTradesAsync(Guid streamId, IEnumerable<string> symbols, CancellationToken cts = default)
    {
      return await Send<SuccessResponse>(new()
      {
        Source = DataUri.AppendPathSegment($"market/trades/subscribe/{streamId}").SetQueryParam("symbols", symbols),
        Action = HttpMethod.Get,
        Cleaner = cts
      });
    }

    /// <summary>Unsubscribe from quotes.</summary>
    public virtual async Task<SuccessResponse> UnsubscribeQuotesAsync(Guid streamId, IEnumerable<string> symbols, CancellationToken cts = default)
    {
      return await Send<SuccessResponse>(new()
      {
        Source = DataUri.AppendPathSegment($"market/quotes/unsubscribe/{streamId}").SetQueryParam("symbols", symbols),
        Action = HttpMethod.Get,
        Cleaner = cts
      });
    }

    /// <summary>Unsubscribe from depths.</summary>
    public virtual async Task<SuccessResponse> UnsubscribeDepthsAsync(Guid streamId, IEnumerable<string> symbols, CancellationToken cts = default)
    {
      return await Send<SuccessResponse>(new()
      {
        Source = DataUri.AppendPathSegment($"market/depths/unsubscribe/{streamId}").SetQueryParam("symbols", symbols),
        Action = HttpMethod.Get,
        Cleaner = cts
      });
    }

    /// <summary>Unsubscribe from trades.</summary>
    public virtual async Task<SuccessResponse> UnsubscribeTradesAsync(Guid streamId, IEnumerable<string> symbols, CancellationToken cts = default)
    {
      return await Send<SuccessResponse>(new()
      {
        Source = DataUri.AppendPathSegment($"market/trades/unsubscribe/{streamId}").SetQueryParam("symbols", symbols),
        Action = HttpMethod.Get,
        Cleaner = cts
      });
    }

    /// <summary>
    /// Web socket stream
    /// </summary>
    /// <param name="uri"></param>
    /// <param name="streamer"></param>
    /// <param name="action"></param>
    protected virtual async Task CreateConnection(string uri, ClientWebSocket streamer, Action<JsonNode> action, CancellationTokenSource cts = null)
    {
      var data = new byte[short.MaxValue];
      var source = new UriBuilder($"{StreamUri}{uri}?token={AccessToken}");

      await streamer
        .ConnectAsync(source.Uri, cts.Token)
        .ConfigureAwait(false);

      var process = new Thread(async o =>
      {
        while (streamer.State is WebSocketState.Open && cts.IsCancellationRequested is false)
        {
          try
          {
            var streamResponse = await streamer.ReceiveAsync(new ArraySegment<byte>(data), cts.Token).ConfigureAwait(false);
            var content = Encoding.UTF8.GetString(data, 0, streamResponse.Count);

            action(JsonNode.Parse(content));
          }
          catch (Exception e)
          {
            OnError(e);
          }
        }
      });

      process.Start();
    }

    #endregion
  }

  #region Supporting Types

  public class SenderQuery
  {
    public string Source { get; set; }
    public object Content { get; set; }
    public CancellationToken? Cleaner { get; set; }
    public HttpMethod Action { get; set; } = HttpMethod.Get;
    public Dictionary<string, string> Headers { get; set; } = new();
    public Dictionary<string, List<string>> ResponseHeaders { get; set; } = new();
  }

  #endregion

  #region DTOs & Enums

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
    [JsonPropertyName("error")] public string ErrorMessage { get; set; }
  }

  public partial class SuccessResponse : Response
  {
    [JsonExtensionData] public Dictionary<string, object> AdditionalProperties { get; set; } = new();
  }

  public partial class TraderInfoResponse : Response
  {
    [JsonPropertyName("accounts")] public List<string> Accounts { get; set; } = [];
    [JsonPropertyName("isLive")] public bool IsLive { get; set; }
    [JsonPropertyName("traderId")] public string TraderId { get; set; }
    [JsonExtensionData] public Dictionary<string, object> AdditionalProperties { get; set; } = new();
  }

  public partial class UserInfoResponse : Response
  {
    [JsonPropertyName("accountCategory")] public int AccountCategory { get; set; }
    [JsonPropertyName("accountTitle")] public string AccountTitle { get; set; }
    [JsonPropertyName("emailAddress1")] public string EmailAddress1 { get; set; }
    [JsonPropertyName("emailAddress2")] public string EmailAddress2 { get; set; }
    [JsonPropertyName("group")] public string Group { get; set; }
    [JsonPropertyName("isClearingAccount")] public bool IsClearingAccount { get; set; }
    [JsonPropertyName("phone1")] public string Phone1 { get; set; }
    [JsonPropertyName("phone2")] public string Phone2 { get; set; }
    [JsonPropertyName("subGroup")] public string SubGroup { get; set; }
    [JsonPropertyName("accounts")] public List<string> Accounts { get; set; } = [];
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
    [EnumMember(Value = "ANY")] ANY = 0,
    [EnumMember(Value = "INVALID")] INVALID = 1,
    [EnumMember(Value = "SUBMITTED")] SUBMITTED = 2,
    [EnumMember(Value = "NEW")] NEW = 3,
    [EnumMember(Value = "PARTIALLY_FILLED")] PARTIALLY_FILLED = 4,
    [EnumMember(Value = "FILLED")] FILLED = 5,
    [EnumMember(Value = "CANCELLED")] CANCELLED = 7,
    [EnumMember(Value = "REJECTED")] REJECTED = 11,
    [EnumMember(Value = "EXPIRED")] EXPIRED = 15,
  }

  public partial class SymbolsResponse : Response
  {
    [JsonPropertyName("symbols")] public List<SymbolInfo> Symbols { get; set; } = [];
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
    [JsonPropertyName("securityDefinitions")] public List<SecurityDefinition> SecurityDefinitions { get; set; } = [];
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

  public partial class SecurityMarginAndValueResponse : Response
  {
    [JsonPropertyName("securityMarginAndValues")] public List<SecurityMarginAndValue> SecurityMarginAndValues { get; set; } = [];
  }

  public partial class SecurityMarginAndValue
  {
    [JsonPropertyName("exchSym")] public string ExchSym { get; set; }
    [JsonPropertyName("currentPrice")] public double CurrentPrice { get; set; }
    [JsonPropertyName("currentTime")] public long CurrentTime { get; set; }
    [JsonPropertyName("currentValue")] public double CurrentValue { get; set; }
    [JsonPropertyName("initialMarginLong")] public double InitialMarginLong { get; set; }
    [JsonPropertyName("initialMaginShort")] public double InitialMaginShort { get; set; }
    [JsonPropertyName("maintMarginLong")] public double MaintMarginLong { get; set; }
    [JsonPropertyName("maintMarginShort")] public double MaintMarginShort { get; set; }
    [JsonPropertyName("spanSettlePrice")] public double SpanSettlePrice { get; set; }
    [JsonPropertyName("spanSettleValue")] public double SpanSettleValue { get; set; }
  }

  public partial class SecurityStatusResponse : Response
  {
    [JsonPropertyName("securityStatuses")] public List<SecurityStatus> SecurityStatuses { get; set; } = [];
  }

  public partial class SecurityStatus
  {
    [JsonPropertyName("exchSym")] public string ExchSym { get; set; }
    [JsonPropertyName("status")] public SecurityStatusType Status { get; set; }
    [JsonPropertyName("statusValue")] public int StatusValue { get; set; }
    [JsonPropertyName("dateTime")] public long DateTime { get; set; }
    [JsonPropertyName("tradeDate")] public long TradeDate { get; set; }
  }

  public enum SecurityStatusType
  {
    [EnumMember(Value = "TRADING_HALT")] TRADING_HALT = 2,
    [EnumMember(Value = "CLOSE")] CLOSE = 18,
    [EnumMember(Value = "PRICE_INDICATION")] PRICE_INDICATION = 15,
    [EnumMember(Value = "OPEN")] OPEN = 17,
    [EnumMember(Value = "CLOSED")] CLOSED = 4,
    [EnumMember(Value = "UNKNOWN")] UNKNOWN = 20,
    [EnumMember(Value = "PRE_OPEN")] PRE_OPEN = 21,
    [EnumMember(Value = "OPENING_ROTATION")] OPENING_ROTATION = 22,
    [EnumMember(Value = "PRE_CROSS")] PRE_CROSS = 24,
    [EnumMember(Value = "CROSS")] CROSS = 25,
    [EnumMember(Value = "NO_CANCEL")] NO_CANCEL = 26,
    [EnumMember(Value = "EXPIRED")] EXPIRED = 30,
    [EnumMember(Value = "PRE_CLOSE")] PRE_CLOSE = 31,
    [EnumMember(Value = "NO_CHANGE")] NO_CHANGE = 103,
    [EnumMember(Value = "POST_CLOSE")] POST_CLOSE = 126,
  }

  public partial class ExchangeSourcesResponse : Response
  {
    [JsonPropertyName("exchanges")] public List<string> Exchanges { get; set; } = [];
  }

  public partial class ComplexesResponse : Response
  {
    [JsonPropertyName("marketComplexes")] public List<ComplexGroups> MarketComplexes { get; set; } = [];
  }

  public partial class ComplexGroups
  {
    [JsonPropertyName("groups")] public List<ComplexGroupInfo> Groups { get; set; } = [];
    [JsonPropertyName("name")] public string Name { get; set; }
  }

  public partial class ComplexGroupInfo
  {
    [JsonPropertyName("group")] public string Group { get; set; }
    [JsonPropertyName("name")] public string Name { get; set; }
  }

  public partial class SymbolFuturesResponse : Response
  {
    [JsonPropertyName("symbols")] public List<FutureInfo> Symbols { get; set; } = [];
  }

  public partial class FutureInfo
  {
    [JsonPropertyName("symbol")] public string Symbol { get; set; }
    [JsonPropertyName("maturityMonth")] public string MaturityMonth { get; set; }
    [JsonPropertyName("maturityYear")] public int MaturityYear { get; set; }
    [JsonPropertyName("description")] public string Description { get; set; }
  }

  public partial class ComplexGroupsResponse : Response
  {
    [JsonPropertyName("symbolGroups")] public List<ComplexGroupInfo> SymbolGroups { get; set; } = [];
  }

  public partial class SymbolOptionsResponse : Response
  {
    [JsonPropertyName("groups")] public List<string> Groups { get; set; } = [];
    [JsonPropertyName("optionGroups")] public List<OptionGroupInfo> OptionGroups { get; set; } = [];
  }

  public partial class OptionGroupInfo
  {
    [JsonPropertyName("group")] public string Group { get; set; }
    [JsonPropertyName("expiration")] public double Expiration { get; set; }
    [JsonPropertyName("description")] public string Description { get; set; }
  }

  public partial class SymbolSearchOptionsResponse : Response
  {
    [JsonPropertyName("symbolOptions")] public List<string> SymbolOptions { get; set; } = [];
  }

  public partial class SymbolOptionSpreadsResponse : Response
  {
    [JsonPropertyName("symbolSpreads")] public List<string> SymbolSpreads { get; set; } = [];
  }

  public partial class StrategyIdResponse : Response
  {
    [JsonPropertyName("Id")] public double Id { get; set; }
    [JsonPropertyName("Minimum")] public double Minimum { get; set; }
    [JsonPropertyName("Maximum")] public double Maximum { get; set; }
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

  public partial class AccountRiskResponse : Response
  {
    [JsonPropertyName("risks")] public List<RiskInfo> Risks { get; set; } = [];
  }

  public partial class RiskInfo
  {
    [JsonPropertyName("accountId")] public string AccountId { get; set; }
    [JsonPropertyName("regCode")] public RegCodeType RegCode { get; set; }
    [JsonPropertyName("currencyCode")] public string CurrencyCode { get; set; }
    [JsonPropertyName("liquidationValue")] public double LiquidationValue { get; set; }
    [JsonPropertyName("startNetLiquidationValue")] public double StartNetLiquidationValue { get; set; }
    [JsonPropertyName("currentNetLiquidationValue")] public double CurrentNetLiquidationValue { get; set; }
    [JsonPropertyName("maxNetLiquidationValue")] public double MaxNetLiquidationValue { get; set; }
    [JsonPropertyName("maxNetLiquidationValueMultiDay")] public double MaxNetLiquidationValueMultiDay { get; set; }
    [JsonPropertyName("liquidationEvents")] public List<long> LiquidationEvents { get; set; } = [];
  }

  public enum RegCodeType
  {
    [EnumMember(Value = "INVALID")] INVALID = 0,
    [EnumMember(Value = "COMBINED")] COMBINED = 1,
    [EnumMember(Value = "REGULATED")] REGULATED = 2,
    [EnumMember(Value = "NON_SECURED")] NON_SECURED = 3,
    [EnumMember(Value = "SECURED")] SECURED = 4,
  }

  public partial class AccountFillsResponse : Response
  {
    [JsonPropertyName("fills")] public List<OrderFill> Fills { get; set; } = [];
  }

  public partial class OrderFill
  {
    [JsonPropertyName("orderId")] public string OrderId { get; set; }
    [JsonPropertyName("strategyId")] public long StrategyId { get; set; }
    [JsonPropertyName("accountId")] public string AccountId { get; set; }
    [JsonPropertyName("exchSym")] public string ExchSym { get; set; }
    [JsonPropertyName("status")] public OrderStatusType Status { get; set; }
    [JsonPropertyName("side")] public OrderSide Side { get; set; }
    [JsonPropertyName("quantity")] public double Quantity { get; set; }
    [JsonPropertyName("price")] public double Price { get; set; }
    [JsonPropertyName("fillQuantity")] public double FillQuantity { get; set; }
    [JsonPropertyName("fillTotalQuantity")] public double FillTotalQuantity { get; set; }
    [JsonPropertyName("fillPrice")] public double FillPrice { get; set; }
    [JsonPropertyName("avgFillPrice")] public double AvgFillPrice { get; set; }
    [JsonPropertyName("fillDate")] public DateTimeOffset FillDate { get; set; }
    [JsonPropertyName("timeOrderEvent")] public long TimeOrderEvent { get; set; }
    [JsonPropertyName("orderUpdateId")] public string OrderUpdateId { get; set; }
  }

  public class AllAccountsResponse : Response
  {
    [JsonPropertyName("accounts")] public List<string> Accounts { get; set; } = [];
  }

  public class AccountsPositionsResponse : Response
  {
    [JsonPropertyName("positions")] public List<Positions> Positions { get; set; } = [];
  }

  public class Positions
  {
    [JsonPropertyName("accountId")] public string AccountId { get; set; }
    [JsonPropertyName("positions")] public List<Position> PositionItems { get; set; } = [];
  }

  public partial class QuotesResponse : Response
  {
    [JsonPropertyName("Quotes")] public List<QuoteFull> Quotes { get; set; } = [];
  }

  public partial class QuoteFull
  {
    [JsonPropertyName("s")] public string S { get; set; }
    [JsonPropertyName("l")] public double L { get; set; }
    [JsonPropertyName("sz")] public int Sz { get; set; }
    [JsonPropertyName("ch")] public double Ch { get; set; }
    [JsonPropertyName("op")] public double Op { get; set; }
    [JsonPropertyName("hi")] public double Hi { get; set; }
    [JsonPropertyName("lo")] public double Lo { get; set; }
    [JsonPropertyName("ags")] public AggressorSideType Ags { get; set; }
    [JsonPropertyName("td")] public TickDirectionType Td { get; set; }
    [JsonPropertyName("stt")] public double Stt { get; set; }
    [JsonPropertyName("stts")] public string Stts { get; set; }
    [JsonPropertyName("sttst")] public long Sttst { get; set; }
    [JsonPropertyName("pstt")] public double Pstt { get; set; }
    [JsonPropertyName("pstts")] public string Pstts { get; set; }
    [JsonPropertyName("sttch")] public double Sttch { get; set; }
    [JsonPropertyName("hb")] public double Hb { get; set; }
    [JsonPropertyName("la")] public double La { get; set; }
    [JsonPropertyName("b")] public double B { get; set; }
    [JsonPropertyName("bt")] public long Bt { get; set; }
    [JsonPropertyName("bs")] public long Bs { get; set; }
    [JsonPropertyName("ibc")] public long Ibc { get; set; }
    [JsonPropertyName("ibs")] public int Ibs { get; set; }
    [JsonPropertyName("a")] public double A { get; set; }
    [JsonPropertyName("at")] public long At { get; set; }
    [JsonPropertyName("as")] public long As { get; set; }
    [JsonPropertyName("ias")] public long Ias { get; set; }
    [JsonPropertyName("iac")] public long Iac { get; set; }
    [JsonPropertyName("tt")] public long Tt { get; set; }
    [JsonPropertyName("tdt")] public string Tdt { get; set; }
    [JsonPropertyName("secs")] public SecurityStatusType Secs { get; set; }
    [JsonPropertyName("sdt")] public string Sdt { get; set; }
    [JsonPropertyName("oi")] public int Oi { get; set; }
    [JsonPropertyName("tv")] public int Tv { get; set; }
    [JsonPropertyName("bv")] public int Bv { get; set; }
    [JsonPropertyName("swv")] public int Swv { get; set; }
    [JsonPropertyName("pv")] public int Pv { get; set; }
  }

  public enum AggressorSideType
  {
    [EnumMember(Value = "INVALID")] INVALID = 0,
    [EnumMember(Value = "BUY")] BUY = 1,
    [EnumMember(Value = "SELL")] SELL = 2,
  }

  public enum TickDirectionType
  {
    [EnumMember(Value = "INVALID")] INVALID = 255,
    [EnumMember(Value = "PLUS")] PLUS = 0,
    [EnumMember(Value = "SAME")] SAME = 1,
    [EnumMember(Value = "MINUS")] MINUS = 2,
  }

  public partial class DepthResponse : Response
  {
    [JsonPropertyName("Depths")] public List<Depth> Depths { get; set; } = [];
  }

  public partial class Depth
  {
    [JsonPropertyName("s")] public string S { get; set; }
    [JsonPropertyName("b")] public List<DepthLevel> B { get; set; } = [];
    [JsonPropertyName("a")] public List<DepthLevel> A { get; set; } = [];
  }

  public partial class DepthLevel
  {
    [JsonPropertyName("l")] public int L { get; set; }
    [JsonPropertyName("t")] public long T { get; set; }
    [JsonPropertyName("s")] public SideShort S { get; set; }
    [JsonPropertyName("p")] public double P { get; set; }
    [JsonPropertyName("o")] public int O { get; set; }
    [JsonPropertyName("sz")] public double Sz { get; set; }
    [JsonPropertyName("ioc")] public int Ioc { get; set; }
    [JsonPropertyName("is")] public double Is { get; set; }
  }

  public class Bar
  {
    [JsonPropertyName("t")] public long T { get; set; }
    [JsonPropertyName("o")] public double O { get; set; }
    [JsonPropertyName("h")] public double H { get; set; }
    [JsonPropertyName("l")] public double L { get; set; }
    [JsonPropertyName("c")] public double C { get; set; }
    [JsonPropertyName("v")] public double V { get; set; }
    [JsonPropertyName("tc")] public long Tc { get; set; }
    [JsonPropertyName("d")] public double D { get; set; }
    [JsonPropertyName("i")] public string I { get; set; }
  }

  public enum SideShort
  {
    [EnumMember(Value = "B")] B = 0,
    [EnumMember(Value = "A")] A = 1,
  }

  public partial class TradesResponse : Response
  {
    [JsonPropertyName("traders")] public List<Trade> Traders { get; set; } = [];
  }

  public partial class Trade
  {
    [JsonPropertyName("symbol")] public string Symbol { get; set; }
    [JsonPropertyName("price")] public double Price { get; set; }
    [JsonPropertyName("change")] public double Change { get; set; }
    [JsonPropertyName("size")] public double Size { get; set; }
    [JsonPropertyName("sequenceNumber")] public double SequenceNumber { get; set; }
    [JsonPropertyName("sendTime")] public long SendTime { get; set; }
    [JsonPropertyName("tickDirection")] public TickDirection TickDirection { get; set; }
    [JsonPropertyName("aggressorSide")] public AggressorSideType AggressorSide { get; set; }
    [JsonPropertyName("tradeDate")] public string TradeDate { get; set; }
    [JsonPropertyName("tradeId")] public double TradeId { get; set; }
    [JsonPropertyName("totalVolume")] public double TotalVolume { get; set; }
  }

  public enum TickDirection
  {
    [EnumMember(Value = "INVALID")] INVALID = 0,
    [EnumMember(Value = "PLUS")] PLUS = 1,
    [EnumMember(Value = "MINUS")] MINUS = 2,
    [EnumMember(Value = "SAME")] SAME = 3,
  }

  public partial class OrdersResponse : Response
  {
    [JsonPropertyName("orders")] public List<Order> Orders { get; set; } = [];
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

  public partial class OrderUpdateRequest
  {
    [JsonPropertyName("orderId")] public string OrderId { get; set; }
    [JsonPropertyName("quantity")] public int Quantity { get; set; }
    [JsonPropertyName("limitPrice")] public double LimitPrice { get; set; }
    [JsonPropertyName("stopPrice")] public double StopPrice { get; set; }
    [JsonPropertyName("stopLoss")] public double StopLoss { get; set; }
    [JsonPropertyName("takeProfit")] public double TakeProfit { get; set; }
    [JsonPropertyName("stopLossOffset")] public float StopLossOffset { get; set; }
    [JsonPropertyName("takeProfitOffset")] public float TakeProfitOffset { get; set; }
    [JsonPropertyName("trailingStop")] public float TrailingStop { get; set; }
  }

  public partial class OrderCancelMultilpleRequest
  {
    [JsonPropertyName("accountId")] public string AccountId { get; set; }
    [JsonPropertyName("orderIds")] public List<string> OrderIds { get; set; } = [];
  }

  public partial class OrdersFillsResponse : Response
  {
    [JsonPropertyName("fills")] public List<OrderFill> Fills { get; set; } = [];
  }

  public partial class SimulatedTraderCreate
  {
    [JsonPropertyName("FirstName")] public string FirstName { get; set; }
    [JsonPropertyName("LastName")] public string LastName { get; set; }
    [JsonPropertyName("Address1")] public string Address1 { get; set; }
    [JsonPropertyName("Address2")] public string Address2 { get; set; }
    [JsonPropertyName("City")] public string City { get; set; }
    [JsonPropertyName("State")] public string State { get; set; }
    [JsonPropertyName("Country")] public string Country { get; set; }
    [JsonPropertyName("ZipCode")] public string ZipCode { get; set; }
    [JsonPropertyName("Phone")] public string Phone { get; set; }
    [JsonPropertyName("Email")] public string Email { get; set; }
    [JsonPropertyName("Password")] public string Password { get; set; }
    [JsonPropertyName("TemplateId")] public string TemplateId { get; set; }
  }

  public partial class SimulatedTraderCreateResponse : Response
  {
    [JsonPropertyName("TraderId")] public string TraderId { get; set; }
  }

  public partial class SimulatedTraderAddAccount
  {
    [JsonPropertyName("TraderId")] public string TraderId { get; set; }
    [JsonPropertyName("Password")] public string Password { get; set; }
    [JsonPropertyName("TemplateId")] public string TemplateId { get; set; }
  }

  public partial class SimulatedTraderAddAccountResponse : Response
  {
    [JsonPropertyName("AccountId")] public string AccountId { get; set; }
  }

  public partial class SimulatedAccountReset
  {
    [JsonPropertyName("AccountId")] public string AccountId { get; set; }
    [JsonPropertyName("TemplateId")] public string TemplateId { get; set; }
  }

  public partial class SimulatedAccountExpire
  {
    [JsonPropertyName("AccountId")] public string AccountId { get; set; }
  }

  public partial class SimulatedAccountAddCash
  {
    [JsonPropertyName("AccountId")] public string AccountId { get; set; }
    [JsonPropertyName("Amount")] public float Amount { get; set; }
  }

  public partial class SimulatedAccountCashReport : Response
  {
    [JsonPropertyName("AccountId")] public string AccountId { get; set; }
    [JsonPropertyName("CashReport")] public List<CashReportEntry> CashReport { get; set; } = [];
  }

  public partial class CashReportEntry
  {
    [JsonPropertyName("amount")] public double Amount { get; set; }
    [JsonPropertyName("entryDate")] public double EntryDate { get; set; }
    [JsonPropertyName("availableDate")] public double AvailableDate { get; set; }
  }

  public partial class SimulatedAccountSetLiquidateOnly
  {
    [JsonPropertyName("AccountId")] public string AccountId { get; set; }
    [JsonPropertyName("LiquidateOnly")] public bool LiquidateOnly { get; set; }
    [JsonPropertyName("TemplateId")] public string TemplateId { get; set; }
  }

  public partial class SimulatedAccountRiskLiquidate
  {
    [JsonPropertyName("Accounts")] public List<string> Accounts { get; set; } = [];
    [JsonPropertyName("Groups")] public List<string> Groups { get; set; } = [];
    [JsonPropertyName("ExceptAccounts")] public List<string> ExceptAccounts { get; set; }
    [JsonPropertyName("ForceManualLiquidation")] public bool ForceManualLiquidation { get; set; }
    [JsonPropertyName("UseManualLiquidationForIlliquidMarkets")] public bool UseManualLiquidationForIlliquidMarkets { get; set; }
    [JsonPropertyName("SendAccountEmail")] public bool SendAccountEmail { get; set; }
    [JsonPropertyName("SendOfficeEmail")] public bool SendOfficeEmail { get; set; }
  }

  public partial class SimulatedAccountSetRisk
  {
    [JsonPropertyName("AccountId")] public string AccountId { get; set; }
    [JsonPropertyName("LiquidationAccountValue")] public double? LiquidationAccountValue { get; set; }
    [JsonPropertyName("LiquidationLossFromStartOfDay")] public double? LiquidationLossFromStartOfDay { get; set; }
    [JsonPropertyName("LiquidationLossFromHighOfDay")] public double? LiquidationLossFromHighOfDay { get; set; }
    [JsonPropertyName("LiquidationLossFromHighOfMultiday")] public double? LiquidationLossFromHighOfMultiday { get; set; }
    [JsonPropertyName("LiquidationPctLossFromStartOfDay")] public double? LiquidationPctLossFromStartOfDay { get; set; }
    [JsonPropertyName("LiquidationPctLossFromHighOfDay")] public double? LiquidationPctLossFromHighOfDay { get; set; }
    [JsonPropertyName("LiquidationPctLossFromHighOfMultiday")] public double? LiquidationPctLossFromHighOfMultiday { get; set; }
    [JsonPropertyName("LiquidationPctMarginDeficiency")] public double? LiquidationPctMarginDeficiency { get; set; }
    [JsonPropertyName("LiquidationMaxValueOverride")] public double? LiquidationMaxValueOverride { get; set; }
    [JsonPropertyName("ReducePositionsOnly")] public bool? ReducePositionsOnly { get; set; }
    [JsonPropertyName("RestoreTrading")] public bool? RestoreTrading { get; set; }
    [JsonPropertyName("MarginScheduleName")] public string MarginScheduleName { get; set; }
    [JsonPropertyName("TemplateId")] public string TemplateId { get; set; }
  }

  public partial class StreamIdResponse : Response
  {
    [JsonPropertyName("streamId")] public Guid StreamId { get; set; }
  }

  public enum OptionType
  {
    [EnumMember(Value = "call")] Call = 0,
    [EnumMember(Value = "put")] Put = 1,
  }

  public class StreamPosition
  {
    [JsonPropertyName("a")] public string A { get; set; }
    [JsonPropertyName("cc")] public string Cc { get; set; }
    [JsonPropertyName("s")] public string S { get; set; }
    [JsonPropertyName("pId")] public string PId { get; set; }
    [JsonPropertyName("q")] public double Q { get; set; }
    [JsonPropertyName("p")] public double P { get; set; }
    [JsonPropertyName("do")] public string Do { get; set; }
    [JsonPropertyName("sd")] public string Sd { get; set; }
    [JsonPropertyName("upl")] public double Upl { get; set; }
  }

  public class StreamAccountPositions
  {
    [JsonPropertyName("a")] public string A { get; set; }
    [JsonPropertyName("p")] public List<StreamPosition> Positions { get; set; } = [];
  }

  public class StreamOrderFill
  {
    [JsonPropertyName("oid")] public string Oid { get; set; }
    [JsonPropertyName("sid")] public long Sid { get; set; }
    [JsonPropertyName("a")] public string A { get; set; }
    [JsonPropertyName("s")] public string S { get; set; }
    [JsonPropertyName("st")] public string St { get; set; }
    [JsonPropertyName("sd")] public string Sd { get; set; }
    [JsonPropertyName("q")] public double Q { get; set; }
    [JsonPropertyName("p")] public double P { get; set; }
    [JsonPropertyName("fq")] public double Fq { get; set; }
    [JsonPropertyName("ftq")] public double Ftq { get; set; }
    [JsonPropertyName("fp")] public double Fp { get; set; }
    [JsonPropertyName("afp")] public double Afp { get; set; }
    [JsonPropertyName("fd")] public DateTimeOffset? Fd { get; set; }
    [JsonPropertyName("t")] public long T { get; set; }
    [JsonPropertyName("ouid")] public string Ouid { get; set; }
  }

  public class StreamRisk
  {
    [JsonPropertyName("a")] public string A { get; set; }
    [JsonPropertyName("rc")] public string Rc { get; set; }
    [JsonPropertyName("cc")] public string Cc { get; set; }
    [JsonPropertyName("lv")] public double Lv { get; set; }
    [JsonPropertyName("snlv")] public double Snlv { get; set; }
    [JsonPropertyName("cnlv")] public double Cnlv { get; set; }
    [JsonPropertyName("mnlv")] public double Mnlv { get; set; }
    [JsonPropertyName("le")] public List<long> Le { get; set; } = [];
  }

  public class StreamOrder
  {
    [JsonPropertyName("oid")] public string Oid { get; set; }
    [JsonPropertyName("sid")] public long Sid { get; set; }
    [JsonPropertyName("poid")] public string Poid { get; set; }
    [JsonPropertyName("a")] public string A { get; set; }
    [JsonPropertyName("s")] public string S { get; set; }
    [JsonPropertyName("st")] public string St { get; set; }
    [JsonPropertyName("sd")] public string Sd { get; set; }
    [JsonPropertyName("q")] public double Q { get; set; }
    [JsonPropertyName("lp")] public double Lp { get; set; }
    [JsonPropertyName("sp")] public double Sp { get; set; }
    [JsonPropertyName("ot")] public string Ot { get; set; }
    [JsonPropertyName("dr")] public string Dr { get; set; }
    [JsonPropertyName("fq")] public double Fq { get; set; }
    [JsonPropertyName("fp")] public double Fp { get; set; }
    [JsonPropertyName("fd")] public DateTimeOffset? Fd { get; set; }
    [JsonPropertyName("cor")] public List<string> Cor { get; set; }
    [JsonPropertyName("err")] public StreamOrderError Error { get; set; }
  }

  public class StreamOrderError
  {
    [JsonPropertyName("errorCode")] public int ErrorCode { get; set; }
    [JsonPropertyName("errorText")] public string ErrorText { get; set; }
  }

  public class StreamTrade
  {
    [JsonPropertyName("s")] public string S { get; set; }
    [JsonPropertyName("p")] public double P { get; set; }
    [JsonPropertyName("ch")] public double Ch { get; set; }
    [JsonPropertyName("sz")] public double Sz { get; set; }
    [JsonPropertyName("sq")] public double Sq { get; set; }
    [JsonPropertyName("st")] public long St { get; set; }
    [JsonPropertyName("td")] public string Td { get; set; }
    [JsonPropertyName("as")] public int As { get; set; }
    [JsonPropertyName("tdt")] public string Tdt { get; set; }
    [JsonPropertyName("tid")] public double Tid { get; set; }
    [JsonPropertyName("is")] public bool Is { get; set; }
    [JsonPropertyName("clx")] public bool Clx { get; set; }
    [JsonPropertyName("spt")] public string Spt { get; set; }
    [JsonPropertyName("ist")] public string Ist { get; set; }
    [JsonPropertyName("bt")] public string Bt { get; set; }
  }

  public class StreamBalance
  {
    [JsonPropertyName("a")] public string A { get; set; }
    [JsonPropertyName("cc")] public string Cc { get; set; }
    [JsonPropertyName("cb")] public double Cb { get; set; }
    [JsonPropertyName("ote")] public double Ote { get; set; }
    [JsonPropertyName("te")] public double Te { get; set; }
    [JsonPropertyName("cba")] public double Cba { get; set; }
    [JsonPropertyName("cbta")] public double Cbta { get; set; }
    [JsonPropertyName("nl")] public double Nl { get; set; }
    [JsonPropertyName("nla")] public double Nla { get; set; }
    [JsonPropertyName("bt")] public string Bt { get; set; }
    [JsonPropertyName("dc")] public double Dc { get; set; }
    [JsonPropertyName("mi")] public StreamMargin Margin { get; set; }
  }

  public class StreamMargin
  {
    [JsonPropertyName("a")] public string A { get; set; }
    [JsonPropertyName("cc")] public string Cc { get; set; }
    [JsonPropertyName("mo")] public StreamMarginDetails Mo { get; set; }
    [JsonPropertyName("mow")] public StreamMarginDetails Mow { get; set; }
    [JsonPropertyName("mowi")] public StreamMarginDetails Mowi { get; set; }
  }

  public class StreamMarginDetails
  {
    [JsonPropertyName("me")] public string Me { get; set; }
    [JsonPropertyName("es")] public string Es { get; set; }
    [JsonPropertyName("irm")] public double Irm { get; set; }
    [JsonPropertyName("mrm")] public double Mrm { get; set; }
    [JsonPropertyName("itm")] public double Itm { get; set; }
    [JsonPropertyName("mtm")] public double Mtm { get; set; }
    [JsonPropertyName("ie")] public bool Ie { get; set; }
    [JsonPropertyName("t")] public long T { get; set; }
  }

  public class StreamIndicator
  {
    [JsonPropertyName("n")] public string N { get; set; }
    [JsonPropertyName("fi")] public int Fi { get; set; }
    [JsonPropertyName("v")] public List<List<string>> V { get; set; } = [];
  }

  public class StreamPing
  {
    [JsonPropertyName("ping")]
    public string Ping { get; set; }
  }

  public class StreamResponse
  {
    [JsonPropertyName("p")]
    public StreamPing Ping { get; set; }

    [JsonPropertyName("q")]
    public List<QuoteFull> Quotes { get; set; } = [];

    [JsonPropertyName("tb")]
    public List<Bar> TradeBars { get; set; } = [];

    [JsonPropertyName("tc")]
    public List<Bar> TickBars { get; set; } = [];

    [JsonPropertyName("ti")]
    public List<Bar> TimeBars { get; set; } = [];

    [JsonPropertyName("vb")]
    public List<Bar> VolumeBars { get; set; } = [];

    [JsonPropertyName("ps")]
    public List<StreamPosition> Positions { get; set; } = [];

    [JsonPropertyName("psa")]
    public List<StreamAccountPositions> AccountPositions { get; set; } = [];

    [JsonPropertyName("f")]
    public List<StreamOrderFill> Fills { get; set; } = [];

    [JsonPropertyName("ri")]
    public StreamRisk Risk { get; set; }

    [JsonPropertyName("ria")]
    public List<StreamRisk> Risks { get; set; } = [];

    [JsonPropertyName("o")]
    public List<StreamOrder> Orders { get; set; } = [];

    [JsonPropertyName("d")]
    public List<Depth> Depths { get; set; } = [];

    [JsonPropertyName("tr")]
    public List<StreamTrade> Trades { get; set; } = [];

    [JsonPropertyName("b")]
    public StreamBalance Balance { get; set; }

    [JsonPropertyName("ba")]
    public List<StreamBalance> Balances { get; set; } = [];

    [JsonPropertyName("i")]
    public List<StreamIndicator> Indicators { get; set; } = [];

    [JsonPropertyName("r")]
    public Response R { get; set; }
  }

  #endregion
}
