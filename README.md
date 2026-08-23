# IronBeam API wrapper

Minimalistic async wrapper around IronBeam API.
A part of a trading framework [Terminal](https://github.com/Indemos/Terminal).

# Status 

![GitHub Workflow Status (with event)](https://img.shields.io/github/actions/workflow/status/Indemos/Terminal/dotnet.yml?event=push)
![GitHub](https://img.shields.io/github/license/Indemos/Terminal)
![GitHub](https://img.shields.io/badge/system-Windows%20%7C%20Linux%20%7C%20Mac-blue)

# Nuget 

`dotnet add package IronBeamBroker`

# Usage 

```C#

// 1. Initialize the HttpClient and IronBeamClient
// Best practice: Reuse a single HttpClient instance throughout your app's lifetime
var client = new IronBeamBroker();

// Optional: Override base URL if you need to point to a simulation environment
// client.BaseUrl = "https://sim.ironbeamapi.com/v2";

// 2. Authorize / Login (Equivalent to SignIn)
Console.WriteLine("Authorizing...");
var loginResponse = await client.AuthorizeAsync(new AuthorizationRequest
{
  Username = "your_username",
  Password = "your_password",
  Apikey = "your_api_key"
});
Console.WriteLine($"Status: {loginResponse.Status}, Token: {loginResponse.Token}");

// 3. Get Trader Info (Equivalent to AccountSearch)
Console.WriteLine("\nFetching trader info...");
// Note: Replace "YOUR_TRADER_ID" with your actual Trader ID
var traderInfo = await client.GetTraderInfoAsync("YOUR_TRADER_ID");
Console.WriteLine($"Is Live: {traderInfo.IsLive}");

var account = traderInfo.Accounts?.FirstOrDefault();
if (string.IsNullOrEmpty(account))
{
  Console.WriteLine("No accounts found for this trader.");
  return;
}
Console.WriteLine($"Using Account ID: {account}");

// 4. Get Symbols (Equivalent to ContractSearch)
Console.WriteLine("\nFetching symbols for 'MES'...");
var symbolsResponse = await client.GetSymbolsAsync(text: "MES", limit: 10);
var symbol = symbolsResponse.Symbols?.FirstOrDefault();
Console.WriteLine($"{symbolsResponse.Symbols?.Count ?? 0} symbol(s) found.");

if (symbol != null)
{
  Console.WriteLine($"First symbol: {symbol.Symbol} - {symbol.Description}");
}

// 5. Get Account Balance
Console.WriteLine("\nFetching account balance...");
var balanceResponse = await client.AccountBalanceAsync(account, BalanceType.CURRENT_OPEN);
Console.WriteLine($"Balance Status: {balanceResponse.Status}");
if (balanceResponse.Balances != null)
{
  foreach (var bal in balanceResponse.Balances)
  {
    Console.WriteLine($"Currency: {bal.CurrencyCode}, Net Liquidity: {bal.NetLiquidity}");
  }
}

// 6. Get Open Positions (Equivalent to PositionSearchOpen)
Console.WriteLine("\nFetching open positions...");
var positionsResponse = await client.PositionsAsync(account);
Console.WriteLine($"{positionsResponse.Positions?.Count ?? 0} position(s) found.");

// 7. Get Orders (Equivalent to OrderSearch)
Console.WriteLine("\nFetching orders...");
var ordersResponse = await client.GetOrdersAsync(account, OrderStatusType.ANY);
Console.WriteLine($"{ordersResponse.Orders?.Count ?? 0} order(s) found.");

// ========================================================================
// 8. Place an Order (Equivalent to OrderPlace) - COMMENTED OUT FOR SAFETY
// ========================================================================

Console.WriteLine("\nPlacing test order...");
var orderRequest = new OrderRequest
{
    AccountId = account,
    ExchSym = symbol?.Symbol ?? "MESZ3", // Replace with actual valid symbol
    Side = OrderSide.Buy,
    Quantity = 1,
    OrderType = OrderType.Limit,
    LimitPrice = 4000.00, // Replace with actual valid price
    Duration = DurationType.DAY,
    WaitForOrderId = true
};

var orderResponse = await client.PlaceOrderAsync(account, orderRequest);
Console.WriteLine($"Order placed successfully. OrderId: {orderResponse.OrderId}");

// ========================================================================
// 9. Cancel the Order (Equivalent to OrderCancel) - COMMENTED OUT FOR SAFETY
// ========================================================================
if (!string.IsNullOrEmpty(orderResponse.OrderId))
{
    Console.WriteLine("\nCancelling test order...");
    var cancelResponse = await client.CancelOrderAsync(account, orderResponse.OrderId);
    Console.WriteLine($"Order cancelled. Status: {cancelResponse.Status}");
}

```