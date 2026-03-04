async function erpCreateOrder(orderPayload) {

    return {
        erpOrderId: `ERP-${Date.now()}`,
        sourceOrderId: orderPayload.id || orderPayload.orderId
    };

}

async function erpUpdateOrderStatus(orderId, status) {
    
    console.log("ERP UPDATE CALLED", orderId, status);

    return {
        orderId,
        newStatus: status
    };

}

async function erpCancelOrder(orderId, reason) {

    console.log("ERP CANCEL CALLED", orderId, reason);

    return {
        orderId,
        cancelled: true,
        reason: reason || "UNKNOWN"
    };

}

module.exports = {
    erpCreateOrder,
    erpUpdateOrderStatus,
    erpCancelOrder
};