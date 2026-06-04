using Microsoft.AspNetCore.SignalR;

namespace DotNetChatServer;

public class ChatHub : Hub
{
    public async Task SendMessage(Message message)
    {
        Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] Received message: {message.Text} at {message.Timestamp}");
        
        // Echo the message back to all clients
        await Clients.All.SendAsync("ReceiveMessage", message);
        
        Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] Message broadcasted to all clients");
    }

    public override async Task OnConnectedAsync()
    {
        Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] Client connected: {Context.ConnectionId}");
        await base.OnConnectedAsync();
    }

    public override async Task OnDisconnectedAsync(Exception? exception)
    {
        Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] Client disconnected: {Context.ConnectionId}");
        if (exception != null)
        {
            Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] Error: {exception.Message}");
        }
        await base.OnDisconnectedAsync(exception);
    }
}
