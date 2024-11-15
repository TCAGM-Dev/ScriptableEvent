export type SignalCallback<K...> = (K...) -> nil

export type ConnectionType = "normal" | "parallel" | "once" | "onceparallel"

export type Connection<K...> = {
    _type: ConnectionType,
    _callback: SignalCallback<K...>,
    Disconnect: (Connection<K...>) -> nil
}

export type Signal<K...> = {
    Connect: <K...>(self: Signal<K...>, callback: SignalCallback<K...>) -> Connection<K...>,
    Once: <K...>(self: Signal<K...>, callback: SignalCallback<K...>) -> Connection<K...>,
    ConnectParallel: <K...>(self: Signal<K...>, callback: SignalCallback<K...>) -> Connection<K...>,
    OnceParallel: <K...>(self: Signal<K...>, callback: SignalCallback<K...>) -> Connection<K...>,
    _connections: {[Connection<K...>]: boolean},
    _disconnectCallback: (Connection<K...>) -> nil,
    _Destroy: () -> nil
}

export type ScriptableEvent<K...> = {
    Fire: <K...>(self: ScriptableEvent<K...>, K...) -> nil,
    Signal: Signal<K...>
}






local Connection = {}
Connection.__index = Connection

function Connection._new<K...>(callback: SignalCallback<K...>, once: boolean, parallel: boolean, disconnectCallback: (Connection<K...>) -> nil): Connection<K...>
    local self = setmetatable({
        _type = if once then (if parallel then "onceparallel" else "once") else (if parallel then "parallel" else "normal"),
        _callback = callback,
        _disconnectCallback = disconnectCallback
    }, Connection)

    return self :: Connection<K...>
end

function Connection:Disconnect()
    self._disconnectCallback(self)
end







local Signal = {}
Signal.__index = Signal

function Signal._new<K...>(): Signal<K...>
    local self = setmetatable({
        _connections = {}
    }, Signal)

    function self._disconnectCallback(connection: Connection<K...>)
        self._connections[connection] = nil
    end

    return self :: Signal<K...>
end

function Signal:_newConnection<K...>(callback: SignalCallback<K...>, once: boolean, parallel: boolean, disconnectCallback: (Connection<K...>) -> nil): Connection<K...>
    local connection = Connection._new(callback, once, parallel, disconnectCallback)
    self._connections[connection] = true
    return connection
end

function Signal:Connect<K...>(callback: SignalCallback<K...>): Connection<K...>
    return self:_newConnection(callback, false, false, self._disconnectCallback)
end
function Signal:Once<K...>(callback: SignalCallback<K...>): Connection<K...>
    return self:_newConnection(callback, true, false, self._disconnectCallback)
end
function Signal:ConnectParallel<K...>(callback: SignalCallback<K...>): Connection<K...>
    return self:_newConnection(callback, false, true, self._disconnectCallback)
end
function Signal:OnceParallel<K...>(callback: SignalCallback<K...>): Connection<K...>
    return self:_newConnection(callback, true, true, self._disconnectCallback)
end

function Signal:_Destroy()
    for connection, _ in pairs(self._connections) do
        connection:Disconnect()
    end
end














local ScriptableEvent = {}
ScriptableEvent.__index = ScriptableEvent

function ScriptableEvent.new<K...>(): ScriptableEvent<K...>
    local self = setmetatable({
        Signal = Signal._new()
    }, ScriptableEvent)

    return self :: ScriptableEvent<K...>
end

local function once<K...>(connection: Connection<K...>, ...: K...)
    connection:Disconnect()
    connection._callback(... :: any)
end
local function parallel<K...>(connection: Connection<K...>, ...: K...)
    task.synchronize()
    connection._callback(... :: any)
    task.desynchronize()
end
local function onceparallel<K...>(connection: Connection<K...>, ...: K...)
    connection:Disconnect()
    task.synchronize()
    connection._callback(... :: any)
    task.desynchronize()
end

function ScriptableEvent:Fire<K...>(...: K...)
    for connection: Connection<K...>, _ in pairs(self.Signal._connections) do
        if connection._type == "normal" then
            task.spawn(connection._callback, ...)
        elseif connection._type == "once" then
            task.spawn(once, connection, ...)
        elseif connection._type == "parallel" then
            task.spawn(parallel, connection, ...)
        elseif connection._type == "onceparallel" then
            task.spawn(onceparallel, connection, ...)
        end
    end
end

function ScriptableEvent:Destroy()
    self.Signal:_Destroy()
    self.Signal = nil
end

return ScriptableEvent