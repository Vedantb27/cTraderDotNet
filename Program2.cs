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
        private static readonly Mode ServerMode = Mode.Demo; // Change to Mode.Live if needed
        private static readonly List<(OpenClient Client, List<IDisposable> Disposables, CancellationTokenSource Cts)> _cTraderConnections = new();
        private static App _app;
        private static Token _token;

        private static async Task Main()
        {
            _app = new App(AppId, AppSecret, string.Empty);

            // Start a single cTrader connection (can be scaled to multiple)
            await StartCTraderConnection();

            // Start the HTTP server in the main thread
            await StartHttpServer();

            // Keep the application running
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

                // Add to connection list for management
                _cTraderConnections.Add((client, disposables, cts));

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
            _cTraderConnections.RemoveAll(c => c.Client == client);
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
            if (_cTraderConnections.Count == 0)
            {
                Console.WriteLine("No active cTrader connections.");
                return;
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

            foreach (var (client, _, _) in _cTraderConnections)
            {
                await client.SendMessage(request);
            }
        }

        private static async void SubscribeToSymbolTrendBar(string[] commandSplit)
        {
            if (_cTraderConnections.Count == 0) return;

            Console.WriteLine("Sending ProtoOASubscribeLiveTrendbarReq...");
            var request = new ProtoOASubscribeLiveTrendbarReq
            {
                Period = (ProtoOATrendbarPeriod)Enum.Parse(typeof(ProtoOATrendbarPeriod), commandSplit[2], true),
                CtidTraderAccountId = long.Parse(commandSplit[3]),
                SymbolId = long.Parse(commandSplit[4]),
            };

            foreach (var (client, _, _) in _cTraderConnections)
            {
                await client.SendMessage(request);
            }
        }

        private static async void SubscribeToSymbolSpot(string[] commandSplit)
        {
            if (_cTraderConnections.Count == 0) return;

            Console.WriteLine("Sending ProtoOASubscribeSpotsReq...");
            var request = new ProtoOASubscribeSpotsReq
            {
                CtidTraderAccountId = long.Parse(commandSplit[2]),
            };
            request.SymbolId.AddRange(commandSplit.Skip(3).Select(iSymbolId => long.Parse(iSymbolId)));

            foreach (var (client, _, _) in _cTraderConnections)
            {
                await client.SendMessage(request);
            }
        }

        private static async void SymbolListRequest(string[] commandSplit)
        {
            if (_cTraderConnections.Count == 0) return;

            var accountId = long.Parse(commandSplit[1]);
            Console.WriteLine("Sending ProtoOASymbolsListReq...");
            var request = new ProtoOASymbolsListReq
            {
                CtidTraderAccountId = accountId,
            };

            foreach (var (client, _, _) in _cTraderConnections)
            {
                await client.SendMessage(request);
            }
        }

        private static async void ReconcileRequest(string[] commandSplit)
        {
            if (_cTraderConnections.Count == 0) return;

            var accountId = long.Parse(commandSplit[1]);
            Console.WriteLine("Sending ProtoOAReconcileReq...");
            var request = new ProtoOAReconcileReq
            {
                CtidTraderAccountId = accountId,
            };

            foreach (var (client, _, _) in _cTraderConnections)
            {
                await client.SendMessage(request);
            }
        }

        private static async void AccountListRequest()
        {
            if (_cTraderConnections.Count == 0) return;

            Console.WriteLine("Sending ProtoOAGetAccountListByAccessTokenReq...");
            var request = new ProtoOAGetAccountListByAccessTokenReq
            {
                AccessToken = _token.AccessToken,
            };

            foreach (var (client, _, _) in _cTraderConnections)
            {
                await client.SendMessage(request);
            }
        }

        private static async void AccountAuthRequest(string[] commandSplit)
        {
            if (_cTraderConnections.Count == 0) return;

            var accountId = long.Parse(commandSplit[1]);
            Console.WriteLine("Sending ProtoOAAccountAuthReq...");
            var request = new ProtoOAAccountAuthReq
            {
                CtidTraderAccountId = accountId,
                AccessToken = _token.AccessToken
            };

            foreach (var (client, _, _) in _cTraderConnections)
            {
                await client.SendMessage(request);
            }
        }

        private static async void TickDataRequest(string[] commandSplit)
        {
            if (_cTraderConnections.Count == 0) return;

            var accountId = long.Parse(commandSplit[1]);
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

            foreach (var (client, _, _) in _cTraderConnections)
            {
                await client.SendMessage(request);
            }
        }

        private static async void TrendbarRequest(string[] commandSplit)
        {
            if (_cTraderConnections.Count == 0) return;

            var period = (ProtoOATrendbarPeriod)Enum.Parse(typeof(ProtoOATrendbarPeriod), commandSplit[1], true);
            var accountId = long.Parse(commandSplit[2]);
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

            foreach (var (client, _, _) in _cTraderConnections)
            {
                await client.SendMessage(request);
            }
        }

        private static async void ProfileRequest()
        {
            if (_cTraderConnections.Count == 0) return;

            Console.WriteLine("Sending ProtoOAGetCtidProfileByTokenReq...");
            var request = new ProtoOAGetCtidProfileByTokenReq
            {
                AccessToken = _token.AccessToken
            };

            foreach (var (client, _, _) in _cTraderConnections)
            {
                await client.SendMessage(request);
            }
        }

        private static void ShowDashLine() => Console.WriteLine("--------------------------------------------------");

        private static void Disconnect()
        {
            Console.WriteLine("Disconnecting all cTrader connections...");
            foreach (var (client, disposables, cts) in _cTraderConnections)
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

        private static async Task StartHttpServer()
        {
            try
            {
                HttpListener listener = new();
                listener.Prefixes.Add("http://localhost:5000/");
                listener.Start();
                Console.WriteLine("HTTP Server listening on http://localhost:5000/");

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

                    var tcs = new TaskCompletionSource<string>();

                    List<IDisposable> subscriptions = new List<IDisposable>();

                    foreach (var (client, _, _) in _cTraderConnections)
                    {
                        var subscription = client.Where(msg => msg is ProtoOAAccountAuthRes || msg is ProtoOAErrorRes)
                            .Subscribe(
                                msg =>
                                {
                                    if (msg is ProtoOAAccountAuthRes authRes)
                                    {
                                        var responseMessage = $"AccountAuth Response for accountId {accountId}: Success";
                                        tcs.TrySetResult(responseMessage);
                                    }
                                    else if (msg is ProtoOAErrorRes errorRes)
                                    {
                                        if (errorRes.ErrorCode == "ALREADY_LOGGED_IN")
                                        {
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
                        await client.SendMessage(authReq);
                    }

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

                    var tcs = new TaskCompletionSource<string>();

                    IDisposable successSubscription = null;
                    IDisposable errorSubscription = null;
                    foreach (var (client, _, _) in _cTraderConnections)
                    {
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
                    }

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

                    var tcs = new TaskCompletionSource<string>();

                    IDisposable successSubscription = null;
                    IDisposable errorSubscription = null;
                    foreach (var (client, _, _) in _cTraderConnections)
                    {
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
                    }

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

                    var tcs = new TaskCompletionSource<string>();

                    IDisposable successSubscription = null;
                    IDisposable errorSubscription = null;
                    foreach (var (client, _, _) in _cTraderConnections)
                    {
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
                    }

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