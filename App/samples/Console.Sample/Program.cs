using Google.Protobuf;
using OpenAPI.Net;
using OpenAPI.Net.Auth;
using OpenAPI.Net.Helpers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Net;
using System.Text.Json;

namespace ConsoleDemo
{
    internal class Program
    {
        private static readonly string AppId = "11608_DdHhy1HVw0kwE1WXrUSZ5ip6j8ro377hROhOyk4pLVmiO9aQmG";
        private static readonly string AppSecret = "QUlHf0Cj2qLspmPXoSmzXadcGqOCyI4xGAkyLvgzL7rEpc4fmj";
        private static readonly Mode ServerMode = Mode.Demo; // Change to Mode.Live if need
        private static readonly List<(OpenClient Client, List<IDisposable> Disposables, CancellationTokenSource Cts)> _cTraderConnections = new();
        private static readonly object _lock = new object();
        private static readonly Dictionary<long, OpenClient> _accountToClient = new();
        private static readonly Dictionary<OpenClient, int> _clientAccountCount = new();
        private static readonly object _mapLock = new object();
        private static App _app;
        private static Token _token;

        private static async Task Main()
        {
            _app = new App(AppId, AppSecret, string.Empty);

            // Start 10 cTrader connections as fire-and-forget async tasks (each running on thread pool threads)
            for (int i = 0; i < 10; i++)
            {
                _ = StartCTraderConnection();
            }

            // Start the HTTP server in the main thread
            await StartHttpServer();

            // Keep the application running (though StartHttpServer() loops infinitely)
            Console.WriteLine("Application running. Press Ctrl+C to exit.");
            await Task.Delay(Timeout.Infinite); // Non-blocking wait 
        }

        private static async Task StartCTraderConnection()
        {
            var cts = new CancellationTokenSource();
            var disposables = new List<IDisposable>();
            OpenClient client = null; // Declare client outside try block
            var useWebSocket = false;

            try
            {
                var host = ApiInfo.GetHost(ServerMode);
                client = new OpenClient(host, ApiInfo.Port, TimeSpan.FromSeconds(10), useWebSocket: useWebSocket);

                // Subscribe to messages
                disposables.Add(client.Where(iMessage => iMessage is not ProtoHeartbeatEvent).Subscribe(OnMessageReceived, OnException));
                disposables.Add(client.OfType<ProtoOARefreshTokenRes>().Subscribe(OnRefreshTokenResponse));

                // Add to connection list for management (thread-safe)
                lock (_lock)
                {
                    _cTraderConnections.Add((client, disposables, cts));
                }

                lock (_mapLock)
                {
                    _clientAccountCount[client] = 0;
                }

                Console.WriteLine("Connecting cTrader Client...");
                await client.Connect();
                ShowDashLine();
                Console.WriteLine("cTrader Client successfully connected");
                ShowDashLine();
                Console.WriteLine("Sending App Auth Req...");
                Console.WriteLine("Please wait...");
                ShowDashLine();

                var applicationAuthReq = new ProtoOAApplicationAuthReq
                {
                    ClientId = _app.ClientId,
                    ClientSecret = _app.Secret,
                };

                await client.SendMessage(applicationAuthReq);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Failed to start cTrader connection: {ex.Message}");
                if (client != null)
                {
                    CleanupConnection(client, disposables, cts);
                }
            }
        }
        private static void CleanupConnection(OpenClient client, List<IDisposable> disposables, CancellationTokenSource cts)
        {
            disposables.ForEach(d => d.Dispose());
            client?.Dispose();
            cts?.Dispose();
            lock (_lock)
            {
                _cTraderConnections.RemoveAll(c => c.Client == client);
            }
            lock (_mapLock)
            {
                var accountsToRemove = _accountToClient.Where(kv => kv.Value == client).Select(kv => kv.Key).ToList();
                foreach (var accountId in accountsToRemove)
                {
                    _accountToClient.Remove(accountId);
                }
                _clientAccountCount.Remove(client);
            }
            Console.WriteLine("cTrader connection cleaned up.");
        }

        private static void OnMessageReceived(IMessage message)
        {
            Console.WriteLine($"\nMessage Received:\n{message}");
            Console.WriteLine();
        }

        private static void OnException(Exception ex)
        {
            Console.WriteLine($"\nException\n: {ex}");
            ShowDashLine();
        }

        private static void OnRefreshTokenResponse(ProtoOARefreshTokenRes response)
        {
            _token = new Token
            {
                AccessToken = response.AccessToken,
                RefreshToken = response.RefreshToken,
                ExpiresIn = DateTimeOffset.FromUnixTimeMilliseconds(response.ExpiresIn),
                TokenType = response.TokenType,
            };

            Console.WriteLine($"New token received: {_token.AccessToken}");
            Console.WriteLine($"As you refreshed your access token, now you have to re-authorize all previously authorized trading accounts");
        }

        private static async void RefreshToken()
        {
            lock (_lock)
            {
                if (_cTraderConnections.Count == 0)
                {
                    Console.WriteLine("No active cTrader connections.");
                    return;
                }
            }

            if (string.IsNullOrWhiteSpace(_token?.RefreshToken))
            {
                Console.Write("Refresh Token: ");
                _token.RefreshToken = Console.ReadLine();
            }

            Console.WriteLine("Sending ProtoOARefreshTokenReq...");
            var request = new ProtoOARefreshTokenReq
            {
                RefreshToken = _token.RefreshToken
            };

            lock (_lock)
            {
                foreach (var (client, _, _) in _cTraderConnections)
                {
                    _ = client.SendMessage(request); // Fire-and-forget to avoid blocking
                }
            }
        }

        private static async void SubscribeToSymbolTrendBar(string[] commandSplit)
        {
            lock (_lock)
            {
                if (_cTraderConnections.Count == 0) return;
            }

            var accountId = long.Parse(commandSplit[3]);
            OpenClient client;
            lock (_mapLock)
            {
                if (!_accountToClient.TryGetValue(accountId, out client))
                {
                    Console.WriteLine($"Account {accountId} not authenticated on any connection.");
                    return;
                }
            }

            Console.WriteLine("Sending ProtoOASubscribeLiveTrendbarReq...");
            var request = new ProtoOASubscribeLiveTrendbarReq
            {
                Period = (ProtoOATrendbarPeriod)Enum.Parse(typeof(ProtoOATrendbarPeriod), commandSplit[2], true),
                CtidTraderAccountId = accountId,
                SymbolId = long.Parse(commandSplit[4]),
            };

            await client.SendMessage(request);
        }

        private static async void SubscribeToSymbolSpot(string[] commandSplit)
        {
            lock (_lock)
            {
                if (_cTraderConnections.Count == 0) return;
            }

            var accountId = long.Parse(commandSplit[2]);
            OpenClient client;
            lock (_mapLock)
            {
                if (!_accountToClient.TryGetValue(accountId, out client))
                {
                    Console.WriteLine($"Account {accountId} not authenticated on any connection.");
                    return;
                }
            }

            Console.WriteLine("Sending ProtoOASubscribeSpotsReq...");
            var request = new ProtoOASubscribeSpotsReq
            {
                CtidTraderAccountId = accountId,
            };
            request.SymbolId.AddRange(commandSplit.Skip(3).Select(iSymbolId => long.Parse(iSymbolId)));

            await client.SendMessage(request);
        }

        private static async void SymbolListRequest(string[] commandSplit)
        {
            lock (_lock)
            {
                if (_cTraderConnections.Count == 0) return;
            }

            var accountId = long.Parse(commandSplit[1]);
            OpenClient client;
            lock (_mapLock)
            {
                if (!_accountToClient.TryGetValue(accountId, out client))
                {
                    Console.WriteLine($"Account {accountId} not authenticated on any connection.");
                    return;
                }
            }

            Console.WriteLine("Sending ProtoOASymbolsListReq...");
            var request = new ProtoOASymbolsListReq
            {
                CtidTraderAccountId = accountId,
            };

            await client.SendMessage(request);
        }

        private static async void ReconcileRequest(string[] commandSplit)
        {
            lock (_lock)
            {
                if (_cTraderConnections.Count == 0) return;
            }

            var accountId = long.Parse(commandSplit[1]);
            OpenClient client;
            lock (_mapLock)
            {
                if (!_accountToClient.TryGetValue(accountId, out client))
                {
                    Console.WriteLine($"Account {accountId} not authenticated on any connection.");
                    return;
                }
            }

            Console.WriteLine("Sending ProtoOAReconcileReq...");
            var request = new ProtoOAReconcileReq
            {
                CtidTraderAccountId = accountId,
            };

            await client.SendMessage(request);
        }

        private static async void AccountListRequest()
        {
            lock (_lock)
            {
                if (_cTraderConnections.Count == 0) return;
            }

            Console.WriteLine("Sending ProtoOAGetAccountListByAccessTokenReq...");
            var request = new ProtoOAGetAccountListByAccessTokenReq
            {
                AccessToken = _token.AccessToken,
            };

            // Since account list is token-based, not account-specific, send to first connection
            OpenClient client;
            lock (_lock)
            {
                client = _cTraderConnections.FirstOrDefault().Client;
                if (client == null) return;
            }

            await client.SendMessage(request);
        }

        private static async void AccountAuthRequest(string[] commandSplit)
        {
            lock (_lock)
            {
                if (_cTraderConnections.Count == 0) return;
            }

            var accountId = long.Parse(commandSplit[1]);

            OpenClient selectedClient;
            lock (_mapLock)
            {
                if (_accountToClient.TryGetValue(accountId, out var existingClient))
                {
                    selectedClient = existingClient;
                }
                else
                {
                    selectedClient = SelectLeastLoadedClient();
                    if (selectedClient == null)
                    {
                        Console.WriteLine("No available connections.");
                        return;
                    }
                }
            }

            Console.WriteLine("Sending ProtoOAAccountAuthReq...");
            var request = new ProtoOAAccountAuthReq
            {
                CtidTraderAccountId = accountId,
                AccessToken = _token.AccessToken
            };

            // Subscribe for response
            var tcs = new TaskCompletionSource<bool>();
            var subscription = selectedClient.Where(msg => msg is ProtoOAAccountAuthRes || msg is ProtoOAErrorRes)
                .Subscribe(
                    msg =>
                    {
                        if (msg is ProtoOAAccountAuthRes)
                        {
                            lock (_mapLock)
                            {
                                if (!_accountToClient.ContainsKey(accountId))
                                {
                                    _accountToClient[accountId] = selectedClient;
                                    _clientAccountCount[selectedClient] = _clientAccountCount.GetValueOrDefault(selectedClient, 0) + 1;
                                }
                            }
                            tcs.TrySetResult(true);
                        }
                        else if (msg is ProtoOAErrorRes errorRes)
                        {
                            if (errorRes.ErrorCode == "ALREADY_LOGGED_IN")
                            {
                                lock (_mapLock)
                                {
                                    if (!_accountToClient.ContainsKey(accountId))
                                    {
                                        _accountToClient[accountId] = selectedClient;
                                        _clientAccountCount[selectedClient] = _clientAccountCount.GetValueOrDefault(selectedClient, 0) + 1;
                                    }
                                }
                                tcs.TrySetResult(true);
                            }
                            else
                            {
                                tcs.TrySetResult(false);
                            }
                        }
                    },
                    ex => tcs.TrySetException(ex));

            await selectedClient.SendMessage(request);

            await tcs.Task;
            subscription.Dispose();
        }

        private static async void TickDataRequest(string[] commandSplit)
        {
            lock (_lock)
            {
                if (_cTraderConnections.Count == 0) return;
            }

            var accountId = long.Parse(commandSplit[1]);
            OpenClient client;
            lock (_mapLock)
            {
                if (!_accountToClient.TryGetValue(accountId, out client))
                {
                    Console.WriteLine($"Account {accountId} not authenticated on any connection.");
                    return;
                }
            }

            var symbolId = long.Parse(commandSplit[2]);
            var type = commandSplit[3].ToLowerInvariant() switch
            {
                "bid" => ProtoOAQuoteType.Bid,
                _ => ProtoOAQuoteType.Ask
            };
            var hours = long.Parse(commandSplit[4]);
            Console.WriteLine("Sending ProtoOAGetTickDataReq...");
            var request = new ProtoOAGetTickDataReq
            {
                CtidTraderAccountId = accountId,
                SymbolId = symbolId,
                FromTimestamp = DateTimeOffset.UtcNow.AddHours(-hours).ToUnixTimeMilliseconds(),
                ToTimestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                Type = type
            };

            await client.SendMessage(request);
        }

        private static async void TrendbarRequest(string[] commandSplit)
        {
            lock (_lock)
            {
                if (_cTraderConnections.Count == 0) return;
            }

            var accountId = long.Parse(commandSplit[2]);
            OpenClient client;
            lock (_mapLock)
            {
                if (!_accountToClient.TryGetValue(accountId, out client))
                {
                    Console.WriteLine($"Account {accountId} not authenticated on any connection.");
                    return;
                }
            }

            var period = (ProtoOATrendbarPeriod)Enum.Parse(typeof(ProtoOATrendbarPeriod), commandSplit[1], true);
            var symbolId = long.Parse(commandSplit[3]);
            var days = long.Parse(commandSplit[4]);
            Console.WriteLine("Sending ProtoOAGetTrendbarsReq...");
            var request = new ProtoOAGetTrendbarsReq
            {
                CtidTraderAccountId = accountId,
                SymbolId = symbolId,
                Period = period,
                FromTimestamp = DateTimeOffset.UtcNow.AddDays(-days).ToUnixTimeMilliseconds(),
                ToTimestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            };
            Console.WriteLine($"FromTimestamp: {request.FromTimestamp} | ToTimestamp: {request.ToTimestamp}");

            await client.SendMessage(request);
        }

        private static async void ProfileRequest()
        {
            lock (_lock)
            {
                if (_cTraderConnections.Count == 0) return;
            }

            Console.WriteLine("Sending ProtoOAGetCtidProfileByTokenReq...");
            var request = new ProtoOAGetCtidProfileByTokenReq
            {
                AccessToken = _token.AccessToken
            };

            // Send to first connection
            OpenClient client;
            lock (_lock)
            {
                client = _cTraderConnections.FirstOrDefault().Client;
                if (client == null) return;
            }

            await client.SendMessage(request);
        }

        private static void ShowDashLine() => Console.WriteLine("--------------------------------------------------");

        private static void Disconnect()
        {
            Console.WriteLine("Disconnecting all cTrader connections...");
            List<(OpenClient, List<IDisposable>, CancellationTokenSource)> connectionsCopy;
            lock (_lock)
            {
                connectionsCopy = new List<(OpenClient, List<IDisposable>, CancellationTokenSource)>(_cTraderConnections);
            }
            foreach (var (client, disposables, cts) in connectionsCopy)
            {
                CleanupConnection(client, disposables, cts);
            }
            Console.WriteLine("Disconnected, exiting...");
            Task.Delay(3000).Wait();
            Environment.Exit(0);
        }

        public static void OpenBrowser(Uri url)
        {
            try
            {
                System.Diagnostics.Process.Start(url.AbsoluteUri);
            }
            catch
            {
                if (System.OperatingSystem.IsWindows())
                {
                    System.Diagnostics.Process.Start(new System.Diagnostics.ProcessStartInfo("cmd", $"/c start {url.AbsoluteUri}") { CreateNoWindow = true });
                }
                else if (System.OperatingSystem.IsLinux())
                {
                    System.Diagnostics.Process.Start("xdg-open", url.AbsoluteUri);
                }
                else if (System.OperatingSystem.IsMacOS())
                {
                    System.Diagnostics.Process.Start("open", url.AbsoluteUri);
                }
                else
                {
                    throw;
                }
            }
        }

        private static OpenClient SelectLeastLoadedClient()
        {
            lock (_lock)
            {
                if (_cTraderConnections.Count == 0) return null;
            }

            lock (_mapLock)
            {
                OpenClient leastLoaded = null;
                int minCount = int.MaxValue;
                foreach (var client in _cTraderConnections.Select(c => c.Client))
                {
                    int count = _clientAccountCount.GetValueOrDefault(client, 0);
                    if (count < minCount)
                    {
                        minCount = count;
                        leastLoaded = client;
                    }
                }
                return leastLoaded;
            }
        }

       private static async Task StartHttpServer()
{
    try
    {
        HttpListener listener = new();
        listener.Prefixes.Add("http://*:5000/");  // Changed to listen on all interfaces
        listener.Start();
        Console.WriteLine("HTTP Server listening on http://*:5000/");

        while (true)
        {
            var context = await listener.GetContextAsync();
            await HandleRequest(context);
        }
    }
    catch (Exception ex)
    {
        Console.WriteLine($"HTTP Server Error: {ex.Message}");
    }
}

        private static async Task HandleRequest(HttpListenerContext context)
        {
            var request = context.Request;
            var response = context.Response;

            try
            {
                // Handle GET request to base route (/)
                if (request.HttpMethod == "GET" && request.Url.AbsolutePath == "/")
                {
                    Console.WriteLine("Received GET request to base route (/)");
                    response.StatusCode = 200;
                    await WriteResponse(response, "Server is running");
                    return;
                }
                // Handle POST request to /account-auth

                // Handle POST request to /account-auth
                else if (request.HttpMethod == "POST" && request.Url.AbsolutePath == "/account-auth")
                {
                    using var reader = new System.IO.StreamReader(request.InputStream, request.ContentEncoding);
                    var body = await reader.ReadToEndAsync();

                    JsonDocument jsonDoc;
                    try
                    {
                        jsonDoc = JsonDocument.Parse(body);
                    }
                    catch (JsonException)
                    {
                        response.StatusCode = 400;
                        await WriteResponse(response, "Invalid JSON format in request body");
                        return;
                    }

                    if (!jsonDoc.RootElement.TryGetProperty("accountId", out var accountIdElement) ||
                        !long.TryParse(accountIdElement.GetString(), out long accountId))
                    {
                        response.StatusCode = 400;
                        await WriteResponse(response, "Invalid or missing 'accountId' in request body");
                        return;
                    }

                    if (!jsonDoc.RootElement.TryGetProperty("accessToken", out var accessTokenElement) ||
                        string.IsNullOrEmpty(accessTokenElement.GetString()))
                    {
                        response.StatusCode = 400;
                        await WriteResponse(response, "Invalid or missing 'accessToken' in request body");
                        return;
                    }
                    var accessToken = accessTokenElement.GetString();

                    Console.WriteLine($"Received HTTP POST request to authenticate account: {accountId}");

                    OpenClient selectedClient;
                    bool noConnectionsAvailable = false;
                    lock (_mapLock)
                    {
                        if (_accountToClient.TryGetValue(accountId, out var existingClient))
                        {
                            selectedClient = existingClient;
                        }
                        else
                        {
                            selectedClient = SelectLeastLoadedClient();
                            if (selectedClient == null)
                            {
                                noConnectionsAvailable = true;
                            }
                        }
                    }

                    if (noConnectionsAvailable)
                    {
                        response.StatusCode = 503;
                        await WriteResponse(response, "No available connections.");
                        return;
                    }

                    var tcs = new TaskCompletionSource<string>();
                    List<IDisposable> subscriptions = new List<IDisposable>();

                    var subscription = selectedClient.Where(msg => msg is ProtoOAAccountAuthRes || msg is ProtoOAErrorRes)
                        .Subscribe(
                            msg =>
                            {
                                if (msg is ProtoOAAccountAuthRes authRes)
                                {
                                    lock (_mapLock)
                                    {
                                        if (!_accountToClient.ContainsKey(accountId))
                                        {
                                            _accountToClient[accountId] = selectedClient;
                                            _clientAccountCount[selectedClient] = _clientAccountCount.GetValueOrDefault(selectedClient, 0) + 1;
                                        }
                                    }
                                    var responseMessage = $"AccountAuth Response for accountId {accountId}: Success";
                                    tcs.TrySetResult(responseMessage);
                                }
                                else if (msg is ProtoOAErrorRes errorRes)
                                {
                                    if (errorRes.ErrorCode == "ALREADY_LOGGED_IN")
                                    {
                                        lock (_mapLock)
                                        {
                                            if (!_accountToClient.ContainsKey(accountId))
                                            {
                                                _accountToClient[accountId] = selectedClient;
                                                _clientAccountCount[selectedClient] = _clientAccountCount.GetValueOrDefault(selectedClient, 0) + 1;
                                            }
                                        }
                                        tcs.TrySetResult($"Account {accountId} is already authenticated");
                                    }
                                    else
                                    {
                                        tcs.TrySetResult($"Error authenticating account {accountId}: {errorRes.ErrorCode} - {errorRes.Description}");
                                    }
                                }
                            },
                            ex =>
                            {
                                tcs.TrySetException(ex);
                            },
                            () =>
                            {
                                // Subscription completed (optional cleanup)
                            });

                    subscriptions.Add(subscription);

                    var authReq = new ProtoOAAccountAuthReq
                    {
                        CtidTraderAccountId = accountId,
                        AccessToken = accessToken
                    };
                    await selectedClient.SendMessage(authReq);

                    var timeoutTask = Task.Delay(TimeSpan.FromSeconds(10));
                    var completedTask = await Task.WhenAny(tcs.Task, timeoutTask);

                    if (completedTask == timeoutTask)
                    {
                        response.StatusCode = 504;
                        await WriteResponse(response, "Timeout waiting for cTrader response");
                        subscriptions.ForEach(s => s.Dispose());
                        return;
                    }

                    if (tcs.Task.IsFaulted)
                    {
                        response.StatusCode = 500;
                        await WriteResponse(response, $"Error: {tcs.Task.Exception?.InnerException?.Message}");
                        subscriptions.ForEach(s => s.Dispose());
                        return;
                    }

                    response.StatusCode = 200;
                    await WriteResponse(response, tcs.Task.Result);
                    subscriptions.ForEach(s => s.Dispose());
                }

                // Handle POST request to /deal-list
                // Handle POST request to /deal-list
                else if (request.HttpMethod == "POST" && request.Url.AbsolutePath == "/deal-list")
                {
                    using var reader = new System.IO.StreamReader(request.InputStream, request.ContentEncoding);
                    var body = await reader.ReadToEndAsync();

                    JsonDocument jsonDoc;
                    try
                    {
                        jsonDoc = JsonDocument.Parse(body);
                    }
                    catch (JsonException)
                    {
                        response.StatusCode = 400;
                        await WriteResponse(response, "Invalid JSON format in request body");
                        return;
                    }

                    if (!jsonDoc.RootElement.TryGetProperty("ctidTraderAccountId", out var accountIdElement) ||
                        !long.TryParse(accountIdElement.GetString(), out long ctidTraderAccountId))
                    {
                        response.StatusCode = 400;
                        await WriteResponse(response, "Invalid or missing 'ctidTraderAccountId' in request body");
                        return;
                    }

                    if (!jsonDoc.RootElement.TryGetProperty("fromTimestamp", out var fromTimestampElement) ||
                        !long.TryParse(fromTimestampElement.GetString(), out long fromTimestamp))
                    {
                        response.StatusCode = 400;
                        await WriteResponse(response, "Invalid or missing 'fromTimestamp' in request body");
                        return;
                    }

                    if (!jsonDoc.RootElement.TryGetProperty("toTimestamp", out var toTimestampElement) ||
                        !long.TryParse(toTimestampElement.GetString(), out long toTimestamp))
                    {
                        response.StatusCode = 400;
                        await WriteResponse(response, "Invalid or missing 'toTimestamp' in request body");
                        return;
                    }

                    Console.WriteLine($"Received HTTP POST request for deal list: ctidTraderAccountId={ctidTraderAccountId}, fromTimestamp={fromTimestamp}, toTimestamp={toTimestamp}");

                    OpenClient client;
                    bool accountNotAuthenticated = false;
                    lock (_mapLock)
                    {
                        if (!_accountToClient.TryGetValue(ctidTraderAccountId, out client))
                        {
                            accountNotAuthenticated = true;
                        }
                    }

                    if (accountNotAuthenticated)
                    {
                        response.StatusCode = 400;
                        await WriteResponse(response, $"Account {ctidTraderAccountId} not authenticated.");
                        return;
                    }

                    var tcs = new TaskCompletionSource<string>();

                    IDisposable successSubscription = null;
                    IDisposable errorSubscription = null;

                    successSubscription = client.OfType<ProtoOADealListRes>()
                        .Where(res => res.CtidTraderAccountId == ctidTraderAccountId)
                        .Take(1)
                        .Subscribe(
                            dealRes =>
                            {
                                var responseMessage = JsonSerializer.Serialize(new
                                {
                                    ctidTraderAccountId = dealRes.CtidTraderAccountId,
                                    deals = dealRes.Deal.Select(deal => new
                                    {
                                        dealId = deal.DealId,
                                        orderId = deal.OrderId,
                                        positionId = deal.PositionId,
                                        volume = deal.Volume,
                                        filledVolume = deal.FilledVolume,
                                        symbolId = deal.SymbolId,
                                        createTimestamp = deal.CreateTimestamp,
                                        executionTimestamp = deal.ExecutionTimestamp,
                                        utcLastUpdateTimestamp = deal.UtcLastUpdateTimestamp,
                                        executionPrice = deal.ExecutionPrice,
                                        tradeSide = deal.TradeSide.ToString(),
                                        dealStatus = deal.DealStatus.ToString(),
                                        marginRate = deal.MarginRate,
                                        commission = deal.Commission,
                                        baseToUsdConversionRate = deal.BaseToUsdConversionRate,
                                        closePositionDetail = deal.ClosePositionDetail != null ? new
                                        {
                                            entryPrice = deal.ClosePositionDetail.EntryPrice,
                                            grossProfit = deal.ClosePositionDetail.GrossProfit,
                                            swap = deal.ClosePositionDetail.Swap,
                                            commission = deal.ClosePositionDetail.Commission,
                                            balance = deal.ClosePositionDetail.Balance,
                                            quoteToDepositConversionRate = deal.ClosePositionDetail.QuoteToDepositConversionRate,
                                            closedVolume = deal.ClosePositionDetail.ClosedVolume,
                                            balanceVersion = deal.ClosePositionDetail.BalanceVersion,
                                            moneyDigits = deal.ClosePositionDetail.MoneyDigits,
                                            pnlConversionFee = deal.ClosePositionDetail.PnlConversionFee
                                        } : null,
                                        moneyDigits = deal.MoneyDigits
                                    }).ToList(),
                                    hasMore = dealRes.HasMore
                                }, new JsonSerializerOptions { WriteIndented = true });
                                tcs.TrySetResult(responseMessage);
                                successSubscription?.Dispose();
                            });

                    errorSubscription = client.OfType<ProtoOAErrorRes>()
                        .Where(err => err.CtidTraderAccountId == ctidTraderAccountId)
                        .Take(1)
                        .Subscribe(
                            errRes =>
                            {
                                var errorMessage = JsonSerializer.Serialize(new
                                {
                                    ctidTraderAccountId = errRes.CtidTraderAccountId,
                                    errorCode = errRes.ErrorCode.ToString(),
                                    description = errRes.Description
                                }, new JsonSerializerOptions { WriteIndented = true });
                                tcs.TrySetResult(errorMessage);
                                errorSubscription?.Dispose();
                            });

                    var dealReq = new ProtoOADealListReq
                    {
                        CtidTraderAccountId = ctidTraderAccountId,
                        FromTimestamp = fromTimestamp,
                        ToTimestamp = toTimestamp
                    };
                    await client.SendMessage(dealReq);

                    var timeoutTask = Task.Delay(TimeSpan.FromSeconds(10));
                    var completedTask = await Task.WhenAny(tcs.Task, timeoutTask);

                    if (completedTask == timeoutTask)
                    {
                        response.StatusCode = 504;
                        await WriteResponse(response, "Timeout waiting for cTrader response");
                        successSubscription?.Dispose();
                        errorSubscription?.Dispose();
                        return;
                    }

                    response.ContentType = "application/json";
                    response.StatusCode = 200;
                    await WriteResponse(response, tcs.Task.Result);
                }
                // Handle POST request to /order-list
                // Handle POST request to /order-list
                else if (request.HttpMethod == "POST" && request.Url.AbsolutePath == "/order-list")
                {
                    using var reader = new System.IO.StreamReader(request.InputStream, request.ContentEncoding);
                    var body = await reader.ReadToEndAsync();

                    JsonDocument jsonDoc;
                    try
                    {
                        jsonDoc = JsonDocument.Parse(body);
                    }
                    catch (JsonException)
                    {
                        response.StatusCode = 400;
                        await WriteResponse(response, "Invalid JSON format in request body");
                        return;
                    }

                    if (!jsonDoc.RootElement.TryGetProperty("ctidTraderAccountId", out var accountIdElement) ||
                        !long.TryParse(accountIdElement.GetString(), out long ctidTraderAccountId))
                    {
                        response.StatusCode = 400;
                        await WriteResponse(response, "Invalid or missing 'ctidTraderAccountId' in request body");
                        return;
                    }

                    bool includeClosed = false;
                    if (jsonDoc.RootElement.TryGetProperty("includeClosed", out var includeClosedElement))
                    {
                        if (!bool.TryParse(includeClosedElement.GetString(), out includeClosed))
                        {
                            response.StatusCode = 400;
                            await WriteResponse(response, "Invalid 'includeClosed' in request body");
                            return;
                        }
                    }

                    long? fromTimestamp = null;
                    if (jsonDoc.RootElement.TryGetProperty("fromTimestamp", out var fromTimestampElement))
                    {
                        if (!long.TryParse(fromTimestampElement.GetString(), out long tempFromTimestamp))
                        {
                            response.StatusCode = 400;
                            await WriteResponse(response, "Invalid 'fromTimestamp' in request body");
                            return;
                        }
                        fromTimestamp = tempFromTimestamp;
                    }

                    long? toTimestamp = null;
                    if (jsonDoc.RootElement.TryGetProperty("toTimestamp", out var toTimestampElement))
                    {
                        if (!long.TryParse(toTimestampElement.GetString(), out long tempToTimestamp))
                        {
                            response.StatusCode = 400;
                            await WriteResponse(response, "Invalid 'toTimestamp' in request body");
                            return;
                        }
                        toTimestamp = tempToTimestamp;
                    }

                    Console.WriteLine($"Received HTTP POST request for order list: ctidTraderAccountId={ctidTraderAccountId}, includeClosed={includeClosed}, fromTimestamp={fromTimestamp}, toTimestamp={toTimestamp}");

                    OpenClient client;
                    bool accountNotAuthenticated = false;
                    lock (_mapLock)
                    {
                        if (!_accountToClient.TryGetValue(ctidTraderAccountId, out client))
                        {
                            accountNotAuthenticated = true;
                        }
                    }

                    if (accountNotAuthenticated)
                    {
                        response.StatusCode = 400;
                        await WriteResponse(response, $"Account {ctidTraderAccountId} not authenticated.");
                        return;
                    }

                    var tcs = new TaskCompletionSource<string>();

                    IDisposable successSubscription = null;
                    IDisposable errorSubscription = null;

                    successSubscription = client.OfType<ProtoOAOrderListRes>()
                        .Where(res => res.CtidTraderAccountId == ctidTraderAccountId)
                        .Take(1)
                        .Subscribe(
                            orderRes =>
                            {
                                var responseMessage = JsonSerializer.Serialize(new
                                {
                                    ctidTraderAccountId = orderRes.CtidTraderAccountId,
                                    orders = orderRes.Order.Select(order => new
                                    {
                                        orderId = order.OrderId,
                                        tradeData = new
                                        {
                                            symbolId = order.TradeData?.SymbolId,
                                            volume = order.TradeData?.Volume,
                                            tradeSide = order.TradeData?.TradeSide.ToString(),
                                            openTimestamp = order.TradeData?.OpenTimestamp,
                                            guaranteedStopLoss = order.TradeData?.GuaranteedStopLoss
                                        },
                                        orderType = order.OrderType.ToString(),
                                        orderStatus = order.OrderStatus.ToString(),
                                        executionPrice = order.ExecutionPrice,
                                        executedVolume = order.ExecutedVolume,
                                        utcLastUpdateTimestamp = order.UtcLastUpdateTimestamp,
                                        closingOrder = order.ClosingOrder,
                                        clientOrderId = order.ClientOrderId,
                                        timeInForce = order.TimeInForce.ToString(),
                                        positionId = order.PositionId
                                    }).ToList(),
                                    hasMore = orderRes.HasMore
                                }, new JsonSerializerOptions { WriteIndented = true });
                                tcs.TrySetResult(responseMessage);
                                successSubscription?.Dispose();
                            });

                    errorSubscription = client.OfType<ProtoOAErrorRes>()
                        .Where(err => err.CtidTraderAccountId == ctidTraderAccountId)
                        .Take(1)
                        .Subscribe(
                            errRes =>
                            {
                                var errorMessage = JsonSerializer.Serialize(new
                                {
                                    ctidTraderAccountId = errRes.CtidTraderAccountId,
                                    errorCode = errRes.ErrorCode.ToString(),
                                    description = errRes.Description
                                }, new JsonSerializerOptions { WriteIndented = true });
                                tcs.TrySetResult(errorMessage);
                                errorSubscription?.Dispose();
                            });

                    var orderReq = new ProtoOAOrderListReq
                    {
                        CtidTraderAccountId = ctidTraderAccountId,
                    };
                    if (fromTimestamp.HasValue)
                        orderReq.FromTimestamp = fromTimestamp.Value;
                    if (toTimestamp.HasValue)
                        orderReq.ToTimestamp = toTimestamp.Value;

                    await client.SendMessage(orderReq);

                    var timeoutTask = Task.Delay(TimeSpan.FromSeconds(10));
                    var completedTask = await Task.WhenAny(tcs.Task, timeoutTask);

                    if (completedTask == timeoutTask)
                    {
                        response.StatusCode = 504;
                        await WriteResponse(response, "Timeout waiting for cTrader response");
                        successSubscription?.Dispose();
                        errorSubscription?.Dispose();
                        return;
                    }

                    response.ContentType = "application/json";
                    response.StatusCode = 200;
                    await WriteResponse(response, tcs.Task.Result);
                }
                // Handle POST request to /symbol-list
                // Handle POST request to /symbol-list
                else if (request.HttpMethod == "POST" && request.Url.AbsolutePath == "/symbol-list")
                {
                    using var reader = new System.IO.StreamReader(request.InputStream, request.ContentEncoding);
                    var body = await reader.ReadToEndAsync();

                    JsonDocument jsonDoc;
                    try
                    {
                        jsonDoc = JsonDocument.Parse(body);
                    }
                    catch (JsonException)
                    {
                        response.StatusCode = 400;
                        await WriteResponse(response, "Invalid JSON format in request body");
                        return;
                    }

                    if (!jsonDoc.RootElement.TryGetProperty("ctidTraderAccountId", out var accountIdElement) ||
                        !long.TryParse(accountIdElement.GetString(), out long ctidTraderAccountId))
                    {
                        response.StatusCode = 400;
                        await WriteResponse(response, "Invalid or missing 'ctidTraderAccountId' in request body");
                        return;
                    }

                    bool includeArchived = false;
                    if (jsonDoc.RootElement.TryGetProperty("includeArchived", out var includeArchivedElement))
                    {
                        if (!bool.TryParse(includeArchivedElement.GetString(), out includeArchived))
                        {
                            response.StatusCode = 400;
                            await WriteResponse(response, "Invalid 'includeArchived' in request body");
                            return;
                        }
                    }

                    Console.WriteLine($"Received HTTP POST request for symbol list: ctidTraderAccountId={ctidTraderAccountId}, includeArchived={includeArchived}");

                    OpenClient client;
                    bool accountNotAuthenticated = false;
                    lock (_mapLock)
                    {
                        if (!_accountToClient.TryGetValue(ctidTraderAccountId, out client))
                        {
                            accountNotAuthenticated = true;
                        }
                    }

                    if (accountNotAuthenticated)
                    {
                        response.StatusCode = 400;
                        await WriteResponse(response, $"Account {ctidTraderAccountId} not authenticated.");
                        return;
                    }

                    var tcs = new TaskCompletionSource<string>();

                    IDisposable successSubscription = null;
                    IDisposable errorSubscription = null;

                    successSubscription = client.OfType<ProtoOASymbolsListRes>()
                        .Where(res => res.CtidTraderAccountId == ctidTraderAccountId)
                        .Take(1)
                        .Subscribe(
                            symbolRes =>
                            {
                                var responseMessage = JsonSerializer.Serialize(new
                                {
                                    ctidTraderAccountId = symbolRes.CtidTraderAccountId,
                                    symbols = symbolRes.Symbol.Select(symbol => new
                                    {
                                        symbolId = symbol.SymbolId,
                                        symbolName = symbol.SymbolName,
                                        enabled = symbol.Enabled,
                                        baseAssetId = symbol.BaseAssetId,
                                        quoteAssetId = symbol.QuoteAssetId,
                                        symbolCategoryId = symbol.SymbolCategoryId,
                                        description = symbol.Description
                                    }).ToList()
                                }, new JsonSerializerOptions { WriteIndented = true });
                                tcs.TrySetResult(responseMessage);
                                successSubscription?.Dispose();
                            });

                    errorSubscription = client.OfType<ProtoOAErrorRes>()
                        .Where(err => err.CtidTraderAccountId == ctidTraderAccountId)
                        .Take(1)
                        .Subscribe(
                            errRes =>
                            {
                                var errorMessage = JsonSerializer.Serialize(new
                                {
                                    ctidTraderAccountId = errRes.CtidTraderAccountId,
                                    errorCode = errRes.ErrorCode.ToString(),
                                    description = errRes.Description
                                }, new JsonSerializerOptions { WriteIndented = true });
                                tcs.TrySetResult(errorMessage);
                                errorSubscription?.Dispose();
                            });

                    var symbolReq = new ProtoOASymbolsListReq
                    {
                        CtidTraderAccountId = ctidTraderAccountId,
                    };
                    await client.SendMessage(symbolReq);

                    var timeoutTask = Task.Delay(TimeSpan.FromSeconds(10));
                    var completedTask = await Task.WhenAny(tcs.Task, timeoutTask);

                    if (completedTask == timeoutTask)
                    {
                        response.StatusCode = 504;
                        await WriteResponse(response, "Timeout waiting for cTrader response");
                        successSubscription?.Dispose();
                        errorSubscription?.Dispose();
                        return;
                    }

                    response.ContentType = "application/json";
                    response.StatusCode = 200;
                    await WriteResponse(response, tcs.Task.Result);
                }

                else
                {
                    response.StatusCode = 404;
                    await WriteResponse(response, "Endpoint not found");
                }
            }
            catch (Exception ex)
            {
                response.StatusCode = 500;
                await WriteResponse(response, $"Error: {ex.Message}");
            }
        }

        private static async Task WriteResponse(HttpListenerResponse response, string content)
        {
            var buffer = System.Text.Encoding.UTF8.GetBytes(content);
            response.ContentLength64 = buffer.Length;
            using var output = response.OutputStream;
            await output.WriteAsync(buffer, 0, buffer.Length);
        }
    }
}