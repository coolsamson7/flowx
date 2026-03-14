# flowx
saga orchestrator

```kt
@SagaType("order")
@StartedBy(PlaceOrderCommand::class)
class OrderSaga : AbstractSaga<OrderSaga>() {

    @Persisted var customerName: String = ""
    @Persisted var amount: Double = 0.0
    @Persisted var txId: String = ""

    @Autowired lateinit var paymentService: PaymentService
    @Autowired lateinit var inventoryService: InventoryService
    @Autowired lateinit var approvalService: ApprovalService

    override fun definition(): SagaDefinition<OrderSaga> = definition

    companion object {
        val definition: SagaDefinition<OrderSaga> = saga {

            // Populate saga state from the triggering command —
            // saga fields are directly in scope (no 'saga.' prefix needed)
            on<PlaceOrderCommand> { cmd ->
                customerName = cmd.customerName
                amount       = cmd.amount
            }

            step("validate") {
                execute   { println("validate order for $customerName ($$amount)") }
                compensate { println("compensate validate") }
            }

            // branch: high-value orders need manual approval, others skip straight through
            branch {
                on({ amount > 1000.0 }) {
                    step("manualApproval") {
                        execute { approvalService.requestApproval(customerName) }
                        compensate { println("compensate manualApproval") }

                        // Receiver IS the saga — access fields directly, no 'saga' param
                        onSuccess<ApprovalGranted> { _ ->
                            println("approval granted for $customerName")
                        }
                        onFailure<ApprovalDenied> { event ->
                            println("approval denied: ${event.reason}")
                        }

                        timeout(60_000)
                    }
                }
                otherwise {
                    // Low-value order: auto-approved, nothing to do
                    step("autoApprove") {
                        execute { println("auto-approved for $customerName") }
                    }
                }
            }

            parallel(join = Join.ALL) {
                step("payment") {
                    execute {
                        paymentService.pay(amount)
                    }
                    compensate { paymentService.compensate() }

                    // Receiver IS the saga — txId is a saga field, set it directly
                    onSuccess<PaymentConfirmed> { event ->
                        txId = event.txId
                        println("payment confirmed txId=$txId")
                    }
                    onFailure<PaymentFailed> { event ->
                        println("payment failed: ${event.reason}")
                    }

                    timeout(30_000)
                }

                step("inventory") {
                    execute   { inventoryService.reserve() }
                    compensate { inventoryService.compensate() }

                    // Only reserve for substantial amounts — step-level condition
                    condition { amount > 0.0 }
                }
            }

            step("shipping") {
                execute {
                    println("shipping order for $customerName (txId=$txId)")
                    // Uncomment to test compensation:
                    // throw RuntimeException("shipping service unavailable")
                }
            }
        }
    }
}
```
