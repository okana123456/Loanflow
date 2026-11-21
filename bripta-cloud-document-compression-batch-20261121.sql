-- Bripta cloud compression manifest: exactly the 200 linked JPEGs reviewed in the SQL audit.
-- No Storage files are changed by this migration.
BEGIN;
CREATE TABLE IF NOT EXISTS public.bripta_document_compression_manifest (
  rank_no integer PRIMARY KEY CHECK (rank_no BETWEEN 1 AND 200),
  object_id uuid NOT NULL UNIQUE,
  kind text NOT NULL,
  expected_bytes bigint NOT NULL CHECK (expected_bytes>1000000),
  business_id text NOT NULL CHECK (business_id='BIZ-B3F5E5D9')
);
ALTER TABLE public.bripta_document_compression_manifest ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON public.bripta_document_compression_manifest FROM PUBLIC,anon,authenticated;
INSERT INTO public.bripta_document_compression_manifest(rank_no,object_id,kind,expected_bytes,business_id) VALUES(1,'92cf7113-b8ed-487b-b7b7-9fffb9258121','client_id_back',5278188,'BIZ-B3F5E5D9'),
(2,'64c41221-a486-439f-9abd-8cf13401605e','client_id_back',5229848,'BIZ-B3F5E5D9'),
(3,'e2a834de-0a04-4817-82a7-4e6f5b65b0db','guarantor_passport',5173183,'BIZ-B3F5E5D9'),
(4,'39fca11d-208f-403c-bbae-b097e522146b','guarantor_id_back',5126280,'BIZ-B3F5E5D9'),
(5,'c9ef2b38-753e-4c1d-9fac-beda97e3bdc1','client_id_front',5047662,'BIZ-B3F5E5D9'),
(6,'2b034daa-4537-47ed-af87-2fe9ba94aed6','client_id_front',4944098,'BIZ-B3F5E5D9'),
(7,'342b34ac-8556-46ff-ba5a-ba8d12918705','guarantor_id_front',4934650,'BIZ-B3F5E5D9'),
(8,'03edb908-73d2-41f0-b6f6-f3f9a1136180','client_passport',4063714,'BIZ-B3F5E5D9'),
(9,'d0e4f650-71a0-44ac-a968-75b6953deb18','guarantor_passport',4018692,'BIZ-B3F5E5D9'),
(10,'767dc4bf-0554-4b1d-98ef-ecf298a20370','client_passport',3818282,'BIZ-B3F5E5D9'),
(11,'16bf5036-d73a-4cf9-a3e5-6e2ce217da5d','client_passport',3796754,'BIZ-B3F5E5D9'),
(12,'96138af2-dd0b-454d-817a-b4abbb4805a5','client_id_back',3779183,'BIZ-B3F5E5D9'),
(13,'130bb0fe-e867-4d0f-b18d-e875c4db6f48','guarantor_id_front',3775329,'BIZ-B3F5E5D9'),
(14,'dd2a6a14-9a87-41b4-b62f-e275428f6bb4','guarantor_passport',3756674,'BIZ-B3F5E5D9'),
(15,'40136379-25f2-4f49-a2d6-02ce8df000e5','guarantor_passport',3742692,'BIZ-B3F5E5D9'),
(16,'f75eec2a-ef3b-4125-acb7-40f17309ae20','guarantor_passport',3729166,'BIZ-B3F5E5D9'),
(17,'41a8558d-847e-49b9-9292-ec2af8d22eae','client_id_back',3681695,'BIZ-B3F5E5D9'),
(18,'49ce34d1-665d-4dde-bdc3-e5e36f911d84','client_id_front',3673405,'BIZ-B3F5E5D9'),
(19,'b43dd043-7f79-48c0-9c68-d2b4be795e23','guarantor_id_front',3643452,'BIZ-B3F5E5D9'),
(20,'de4891a7-26fa-40d7-98c8-40855380277c','client_passport',3639574,'BIZ-B3F5E5D9'),
(21,'638f4837-e330-4c46-bb8d-8dccd60e688f','client_passport',3636241,'BIZ-B3F5E5D9'),
(22,'646aae33-0772-47da-9d96-ebe32c2cba31','guarantor_id_back',3596019,'BIZ-B3F5E5D9'),
(23,'84fe4e26-cb28-4a45-8bf2-30151fcd4043','client_id_back',3538168,'BIZ-B3F5E5D9'),
(24,'cf12ae3f-c3ba-412d-907d-bcea02774124','client_passport',3534861,'BIZ-B3F5E5D9'),
(25,'21a28272-d4d3-46b3-a604-ebdb83ddcaae','guarantor_id_front',3503813,'BIZ-B3F5E5D9'),
(26,'d3ad1ac5-da48-4bbe-add5-eb6884d8a50b','guarantor_id_back',3481419,'BIZ-B3F5E5D9'),
(27,'b13c8c89-bbd7-4071-b811-e6d50c869099','guarantor_passport',3438231,'BIZ-B3F5E5D9'),
(28,'b48309b4-ff49-43aa-baff-49c5a36832d9','client_passport',3434040,'BIZ-B3F5E5D9'),
(29,'e0848613-5e15-4fe0-8611-db1eeaad669a','guarantor_passport',3401403,'BIZ-B3F5E5D9'),
(30,'5acd1399-c902-4280-aa7a-b463b9abab6f','client_passport',3397779,'BIZ-B3F5E5D9'),
(31,'605a269d-23d5-457c-9b4f-72a6d1d805b9','client_id_back',3396501,'BIZ-B3F5E5D9'),
(32,'88decd22-e42c-4403-9130-24402b34df6c','client_id_front',3370472,'BIZ-B3F5E5D9'),
(33,'2a93be32-e8c0-4797-95c8-8afe6e093b52','client_id_front',3367746,'BIZ-B3F5E5D9'),
(34,'9c687c02-eaef-4697-a999-318a0195c1fe','client_id_front',3366642,'BIZ-B3F5E5D9'),
(35,'df1bc5be-c2f7-482f-975e-b56a0a708de4','guarantor_id_front',3357435,'BIZ-B3F5E5D9'),
(36,'aa757bc1-585d-4cdb-8ac9-f793c993b83e','client_passport',3344449,'BIZ-B3F5E5D9'),
(37,'92f836a2-bb2f-45a7-bac6-ba49af76c0e4','client_passport',3340250,'BIZ-B3F5E5D9'),
(38,'c90fa785-e511-409d-953c-1dba4d46831e','client_passport',3329690,'BIZ-B3F5E5D9'),
(39,'3190310f-557f-4dfd-8bd0-2d87d17d7f37','guarantor_passport',3305670,'BIZ-B3F5E5D9'),
(40,'302ef1e9-5872-40a9-8df9-40f38aa59ce0','guarantor_id_front',3297924,'BIZ-B3F5E5D9'),
(41,'5ef61f2d-057e-46d8-afae-51748bc3243e','client_id_front',3276577,'BIZ-B3F5E5D9'),
(42,'5131497e-5a68-496e-9b36-9386d7f2ac12','guarantor_id_front',3270867,'BIZ-B3F5E5D9'),
(43,'c5c09e92-4674-4e30-a8e9-d8cb6f8acd41','client_id_front',3267623,'BIZ-B3F5E5D9'),
(44,'28a69c8d-5fb2-4f9f-8707-75f4d74484c7','guarantor_passport',3230692,'BIZ-B3F5E5D9'),
(45,'83671e98-1dcb-45cf-b016-ea792361dfb2','guarantor_passport',3227027,'BIZ-B3F5E5D9'),
(46,'626eb1aa-8319-4820-a28c-f45b186f8917','guarantor_passport',3222710,'BIZ-B3F5E5D9'),
(47,'b8e8fc57-6ab5-42c2-8b9f-9aacd609ffc2','guarantor_id_back',3203349,'BIZ-B3F5E5D9'),
(48,'cb98f0e2-16e9-4263-9515-b2f2aa6158b2','guarantor_passport',3193457,'BIZ-B3F5E5D9'),
(49,'38b525dd-7fe5-4c1b-8742-c8e613640eda','client_id_back',3191166,'BIZ-B3F5E5D9'),
(50,'1c3d0772-10f4-4274-9b77-4f208f1b3870','guarantor_passport',3182637,'BIZ-B3F5E5D9'),
(51,'546bb3c5-cdf0-4f40-86d6-0cf55de208ef','client_id_back',3166000,'BIZ-B3F5E5D9'),
(52,'98b044b7-6afb-439a-80da-b344a0b09b89','client_id_front',3153339,'BIZ-B3F5E5D9'),
(53,'5d1c7523-3289-4019-94f6-77dbc65ef465','client_passport',3150346,'BIZ-B3F5E5D9'),
(54,'a576680a-c102-46dd-9fd7-a4e369abee70','guarantor_id_front',3137088,'BIZ-B3F5E5D9'),
(55,'2944583d-d171-4cc3-a118-63ce1c315be1','client_id_front',3133933,'BIZ-B3F5E5D9'),
(56,'8f9e320f-ab27-4dd9-90bb-f4f914d5da8f','guarantor_passport',3129674,'BIZ-B3F5E5D9'),
(57,'545b5c3f-008b-41ad-bc53-3cdbf46d66cf','client_id_back',3127184,'BIZ-B3F5E5D9'),
(58,'ffcbe739-6528-4918-a841-b176687c6d89','guarantor_id_back',3123857,'BIZ-B3F5E5D9'),
(59,'f82e218b-c64e-4f9d-b5ae-2dc91b0e15f7','client_passport',3111134,'BIZ-B3F5E5D9'),
(60,'45d2d367-0019-48b3-af9f-7d4e1d2ec56e','client_id_back',3105676,'BIZ-B3F5E5D9'),
(61,'7189315c-8e71-4b09-b720-f285f27c3732','guarantor_id_back',3105509,'BIZ-B3F5E5D9'),
(62,'f7af381c-3387-47ab-a24a-71ec724ca7c6','client_id_front',3101033,'BIZ-B3F5E5D9'),
(63,'d0e676af-acfc-4d2e-b79d-aa0825dab5d1','client_id_front',3088177,'BIZ-B3F5E5D9'),
(64,'a9d3e913-9a2d-48da-9628-b66a2932cdc1','client_id_front',3074896,'BIZ-B3F5E5D9'),
(65,'675f1268-f700-408e-9dff-5caeb79348f7','client_id_back',3074589,'BIZ-B3F5E5D9'),
(66,'7eb3029e-9405-4573-82a4-dec961be6519','guarantor_id_front',3063952,'BIZ-B3F5E5D9'),
(67,'208b6cd6-c6f9-4dda-9c66-8a7837da4d07','guarantor_id_front',3058967,'BIZ-B3F5E5D9'),
(68,'71c0dde9-4d27-4c8a-a9ab-46f9af73348a','client_id_back',3056059,'BIZ-B3F5E5D9'),
(69,'7466f082-df35-4808-8ac6-c952eb74ae7c','client_id_front',3056059,'BIZ-B3F5E5D9'),
(70,'f81cf7e3-0aa0-47ba-8dee-461aaa33c0d4','client_passport',3055966,'BIZ-B3F5E5D9'),
(71,'f839c9ff-6f3e-469c-befa-740efa449f41','client_id_back',3054389,'BIZ-B3F5E5D9'),
(72,'c378e6ea-40a6-4ad2-b401-447349e9c8b5','guarantor_id_front',3049813,'BIZ-B3F5E5D9'),
(73,'53cc252b-ddab-4467-af17-70977f3da12b','client_id_front',3038048,'BIZ-B3F5E5D9'),
(74,'e29a9d99-b28b-403d-8293-54321607244b','guarantor_id_back',3035827,'BIZ-B3F5E5D9'),
(75,'f37fe8a6-0847-4f90-9dd7-0437276e4e6d','guarantor_passport',3034777,'BIZ-B3F5E5D9'),
(76,'93e65af8-e4f9-46dd-b4a2-4eac41ec58aa','guarantor_id_back',3033172,'BIZ-B3F5E5D9'),
(77,'8eecbac4-6572-439f-901e-3a5f398b32ee','client_id_back',3030983,'BIZ-B3F5E5D9'),
(78,'4ec9b5ac-98ee-448f-8d24-428f57909491','guarantor_id_back',3028920,'BIZ-B3F5E5D9'),
(79,'2a0b8c40-b3b8-47dc-9afb-f7a833e0cacf','client_passport',3028793,'BIZ-B3F5E5D9'),
(80,'3d051fa9-2d60-48a2-83db-8ece95489108','client_id_back',3027682,'BIZ-B3F5E5D9'),
(81,'d0f08f5f-cc01-4630-bdef-bc57cb019bfe','client_id_back',3024612,'BIZ-B3F5E5D9'),
(82,'38950d08-56f8-40c2-9f5e-ef13a127e254','guarantor_passport',3024266,'BIZ-B3F5E5D9'),
(83,'f597e8b8-5b7e-43a1-91b5-ec4b4d5abf11','guarantor_id_front',3023980,'BIZ-B3F5E5D9'),
(84,'c3ea6e41-28a6-45e5-847d-9b1e4cb46dad','guarantor_id_back',3018675,'BIZ-B3F5E5D9'),
(85,'db6862b0-9649-4c24-8591-34690ea56ea2','guarantor_id_back',3014532,'BIZ-B3F5E5D9'),
(86,'59c3ee35-c665-4b82-afc4-ab5aedcaf945','client_id_front',3009440,'BIZ-B3F5E5D9'),
(87,'732e39b2-ca19-4fe0-b137-24ea6bf9a0d2','guarantor_id_front',3008664,'BIZ-B3F5E5D9'),
(88,'fbb1b985-4f1c-4af6-84ed-1916e4f0233b','guarantor_id_front',3007734,'BIZ-B3F5E5D9'),
(89,'fa8e5b8d-e814-4c11-83d8-4c86d3f64580','client_id_back',3007085,'BIZ-B3F5E5D9'),
(90,'32c25fa8-b99c-460f-911d-54cf5a1f19c9','guarantor_id_back',3001144,'BIZ-B3F5E5D9'),
(91,'62dacab1-c315-4745-8a0b-9c738918b3a0','client_id_back',2999555,'BIZ-B3F5E5D9'),
(92,'b87e2b38-30e6-4956-8780-93f870cdccd2','guarantor_id_front',2987504,'BIZ-B3F5E5D9'),
(93,'3aef3fb8-eb73-467c-bfe3-d556c069c065','client_id_back',2982973,'BIZ-B3F5E5D9'),
(94,'cbf96003-82f0-4c56-b1a6-acecc7c599ba','guarantor_id_back',2979086,'BIZ-B3F5E5D9'),
(95,'5615b918-eb0c-486b-a9be-f8f184cef83b','client_id_back',2974435,'BIZ-B3F5E5D9'),
(96,'03dea64d-c22d-4724-8d2c-fd3ba36cb65c','guarantor_id_back',2974052,'BIZ-B3F5E5D9'),
(97,'da2e9ef2-3768-409e-aba0-0a856ed0c765','client_id_front',2957599,'BIZ-B3F5E5D9'),
(98,'02a4104c-284b-44e0-bdec-d3caf8c3c6d1','guarantor_passport',2955204,'BIZ-B3F5E5D9'),
(99,'85643529-47a2-41f2-99dc-7688aa569b34','client_id_front',2952939,'BIZ-B3F5E5D9'),
(100,'e37678e2-1401-416d-b45a-373845fb94ec','guarantor_passport',2945098,'BIZ-B3F5E5D9'),
(101,'840a1275-f09a-446b-8869-a2b4c8b0d4d4','guarantor_passport',2943190,'BIZ-B3F5E5D9'),
(102,'5b74178f-6e64-437c-ac6a-aaabfe640781','guarantor_id_front',2940434,'BIZ-B3F5E5D9'),
(103,'2d6e1f4f-4171-4431-adb9-5ca0d162def9','client_id_front',2937634,'BIZ-B3F5E5D9'),
(104,'38a4290a-8738-4474-b7fd-a6de29580a6c','client_id_back',2930850,'BIZ-B3F5E5D9'),
(105,'c46cef32-0710-4eec-b0cd-835c8a0758d9','guarantor_id_back',2928498,'BIZ-B3F5E5D9'),
(106,'6c379864-8388-4c06-9ed7-e485d3793576','guarantor_id_back',2919820,'BIZ-B3F5E5D9'),
(107,'661e5d0f-b37e-4e37-9f1a-2fbf99ccb959','guarantor_passport',2912413,'BIZ-B3F5E5D9'),
(108,'a3097154-b7d7-465c-9a16-57637753730a','guarantor_id_front',2912413,'BIZ-B3F5E5D9'),
(109,'8bd29552-ebc9-487b-a619-0d43d56cd6b1','guarantor_id_back',2904380,'BIZ-B3F5E5D9'),
(110,'04db978e-4dee-45cd-a647-4197b6319e14','guarantor_id_back',2894398,'BIZ-B3F5E5D9'),
(111,'68cef6b0-3bd2-479b-8e02-3e64ab4fdb9a','client_passport',2892874,'BIZ-B3F5E5D9'),
(112,'65df5f19-a0e8-4fe6-b7ad-08fecf3d0ed8','client_id_back',2888913,'BIZ-B3F5E5D9'),
(113,'5eae583b-cb64-4c06-afa5-8f3c84efd9cf','client_passport',2887933,'BIZ-B3F5E5D9'),
(114,'1dfa807b-79e1-457e-90fa-0b2923edb7ed','guarantor_passport',2882082,'BIZ-B3F5E5D9'),
(115,'f4140e0f-d331-4149-bb96-42a60fa16cb1','guarantor_id_back',2881432,'BIZ-B3F5E5D9'),
(116,'f9124b87-930f-4768-a1ea-cbd6a75d87cc','client_id_back',2881337,'BIZ-B3F5E5D9'),
(117,'e531be01-4e1e-429d-a173-5cfacc6dca86','client_id_back',2880783,'BIZ-B3F5E5D9'),
(118,'7f833277-5bdf-448b-9410-bd89a499a68a','client_id_back',2875034,'BIZ-B3F5E5D9'),
(119,'1611b6a4-d0d0-4e77-a832-64eeca180afa','guarantor_id_back',2871007,'BIZ-B3F5E5D9'),
(120,'f2a48a24-1610-4971-8cee-f90c6e26ca74','guarantor_id_front',2869156,'BIZ-B3F5E5D9'),
(121,'f8fd0001-c44f-4755-8f72-eb8a891472a8','client_id_front',2864302,'BIZ-B3F5E5D9'),
(122,'153db973-fdc8-489f-9784-ec5404183f7d','client_id_back',2858619,'BIZ-B3F5E5D9'),
(123,'6fcf7bba-54b0-4d98-ba36-ec942020f1fc','client_passport',2850277,'BIZ-B3F5E5D9'),
(124,'d1dbd5d7-0167-4904-b725-6c2812de40f5','client_id_front',2847879,'BIZ-B3F5E5D9'),
(125,'2e43ed7e-8070-4909-90d6-c6806f74281a','client_id_back',2834699,'BIZ-B3F5E5D9'),
(126,'5ee7e1c2-e18f-476e-b015-e97c80c5b6f3','client_id_front',2829967,'BIZ-B3F5E5D9'),
(127,'999379ae-1c21-4fcd-9fe4-3f8a3b838129','client_id_back',2820391,'BIZ-B3F5E5D9'),
(128,'71f2aa74-bdd2-42a1-9a15-0cc3f609197e','client_id_front',2815025,'BIZ-B3F5E5D9'),
(129,'8c8dbab9-25a5-472b-a403-f3ae146b8fa7','client_id_front',2814216,'BIZ-B3F5E5D9'),
(130,'29dc8db5-f33e-4bd2-969b-872b75691e9c','guarantor_id_front',2809936,'BIZ-B3F5E5D9'),
(131,'fb720998-0a60-4887-915a-242013ce71ce','client_id_back',2805078,'BIZ-B3F5E5D9'),
(132,'17b2a21a-5cad-491b-9408-6e4b7d407b53','guarantor_id_back',2802136,'BIZ-B3F5E5D9'),
(133,'8caffc09-85f3-4a6b-b01c-e17e9eee83da','client_id_front',2798577,'BIZ-B3F5E5D9'),
(134,'f03d470a-8924-4998-ba0d-eb458f491c20','guarantor_id_front',2794549,'BIZ-B3F5E5D9'),
(135,'ae8f6f65-329a-41ee-9526-9c7ad3775ce2','guarantor_id_front',2791475,'BIZ-B3F5E5D9'),
(136,'5456c89f-7726-494b-8df8-ad50f9edb99c','guarantor_id_front',2791344,'BIZ-B3F5E5D9'),
(137,'e6f191bb-5c6b-44a4-a626-09e8f2ad5171','guarantor_id_front',2785951,'BIZ-B3F5E5D9'),
(138,'7f44272d-0dd0-4353-a656-53347b176443','guarantor_id_back',2785220,'BIZ-B3F5E5D9'),
(139,'a0d1e0b2-cc4c-487c-aa10-0212ee54ffca','client_id_back',2781849,'BIZ-B3F5E5D9'),
(140,'c8c42706-357a-4564-8036-1992470e1a4d','guarantor_id_back',2776947,'BIZ-B3F5E5D9'),
(141,'b55b68ee-fcac-48db-a76d-196217fc3fd1','guarantor_id_front',2776141,'BIZ-B3F5E5D9'),
(142,'459aa664-c00c-4e5e-b8fa-f3dc1141c27e','client_id_back',2771397,'BIZ-B3F5E5D9'),
(143,'91476591-e8f3-4b81-ae87-612370cf6ce9','guarantor_id_back',2763567,'BIZ-B3F5E5D9'),
(144,'0dc63dcb-be3b-4d05-938e-49f14749c53d','client_id_back',2759914,'BIZ-B3F5E5D9'),
(145,'a8976a42-fa0d-4d52-b9f1-e5e8c2378bbe','guarantor_id_back',2755858,'BIZ-B3F5E5D9'),
(146,'22ab6d28-207d-493e-8f1f-11e06e5dd4db','client_id_front',2754939,'BIZ-B3F5E5D9'),
(147,'737e2355-2933-4e9e-ad0c-7ce92c215717','client_id_back',2752237,'BIZ-B3F5E5D9'),
(148,'12472bdb-e679-4e46-83b9-f0dd93bb81b6','client_id_back',2750947,'BIZ-B3F5E5D9'),
(149,'21c84610-995a-493e-84f4-55afbc6a2062','guarantor_id_back',2746671,'BIZ-B3F5E5D9'),
(150,'ff19ee03-e66e-41fa-bc7a-c125e5a7279a','guarantor_id_back',2744534,'BIZ-B3F5E5D9'),
(151,'1f06e743-7f30-48af-8f7f-29ab56ffb506','guarantor_passport',2739132,'BIZ-B3F5E5D9'),
(152,'1b332fd6-4fdc-4052-a132-1a3d6e28416f','client_id_front',2735724,'BIZ-B3F5E5D9'),
(153,'ca50acde-a467-4dcc-89dc-7be042f825d6','guarantor_passport',2729922,'BIZ-B3F5E5D9'),
(154,'9a992abe-1c23-40df-ba82-a0b1486ba014','guarantor_passport',2724941,'BIZ-B3F5E5D9'),
(155,'05aa7d54-7adf-4c4b-b3e6-68b29b110034','client_id_front',2724583,'BIZ-B3F5E5D9'),
(156,'0910de9c-6d70-46e7-bfff-48f75a26e3ea','client_id_back',2722695,'BIZ-B3F5E5D9'),
(157,'d3bbae7b-a7b5-4907-a995-a1959f1feb88','client_passport',2719331,'BIZ-B3F5E5D9'),
(158,'93bc7680-d01c-46e3-94f8-f6cf3158461a','guarantor_id_back',2714654,'BIZ-B3F5E5D9'),
(159,'6a82742a-9108-4164-bf36-1f1f6bb9eb73','client_id_back',2712301,'BIZ-B3F5E5D9'),
(160,'48f8a9c2-0200-4dbb-8261-e1741341d4cb','guarantor_id_back',2712013,'BIZ-B3F5E5D9'),
(161,'b98da837-4e3f-4b8d-9a76-2e3bb8ed80cd','guarantor_id_front',2711646,'BIZ-B3F5E5D9'),
(162,'28cfe85e-8972-4bad-80df-5f421caa04af','client_id_front',2706464,'BIZ-B3F5E5D9'),
(163,'736c11a1-6020-407d-b0f2-ea2a16d0cbaf','client_id_back',2704590,'BIZ-B3F5E5D9'),
(164,'04202a4d-e0fe-4823-82b9-12b1876e391d','client_id_back',2684977,'BIZ-B3F5E5D9'),
(165,'37475f03-0056-4bc9-87b3-3806a02482b8','guarantor_passport',2684542,'BIZ-B3F5E5D9'),
(166,'fd671235-d944-450b-bf18-e4fcdb328c03','client_id_front',2678894,'BIZ-B3F5E5D9'),
(167,'96f31d61-1f59-4132-8b3f-b319a348f170','client_id_front',2661095,'BIZ-B3F5E5D9'),
(168,'7035ea65-7edd-4afc-bcd7-5ad50aa6bbb3','client_passport',2658969,'BIZ-B3F5E5D9'),
(169,'67829acf-5561-4ab1-a3ad-7ca44a8b6722','guarantor_id_back',2655017,'BIZ-B3F5E5D9'),
(170,'1760b46d-538f-4637-91fb-b5fa01ecfc2d','client_id_front',2654368,'BIZ-B3F5E5D9'),
(171,'fe1bf453-9b84-4fc7-838d-8e8bc082f065','guarantor_id_back',2651697,'BIZ-B3F5E5D9'),
(172,'12c6c4e2-74d4-49c1-a921-4253f472ccce','guarantor_passport',2650468,'BIZ-B3F5E5D9'),
(173,'c39c104f-0bce-4ca6-9330-5a85a381f4bb','client_passport',2642896,'BIZ-B3F5E5D9'),
(174,'c3f20c07-70f2-4714-ad13-75ba545a13fc','guarantor_passport',2635211,'BIZ-B3F5E5D9'),
(175,'10c4faf3-6a07-4ae1-8b29-8f24f5aee868','guarantor_id_front',2613830,'BIZ-B3F5E5D9'),
(176,'57af6d4b-19d3-4d87-9e0d-fb481505d249','client_id_back',2613026,'BIZ-B3F5E5D9'),
(177,'73cc8ed6-a328-45fb-bfe0-8608c6f4fea3','guarantor_id_front',2607896,'BIZ-B3F5E5D9'),
(178,'8fba73cc-f2fc-46d9-a82f-6f38bc6d31c2','client_passport',2601188,'BIZ-B3F5E5D9'),
(179,'92162c0c-9da8-4617-aa97-631ece5156ab','guarantor_passport',2597155,'BIZ-B3F5E5D9'),
(180,'cfeaae26-45c0-4cd1-8764-c20d9c705c21','guarantor_id_back',2592831,'BIZ-B3F5E5D9'),
(181,'d133f742-e5bd-4f13-ae52-6e1131874a06','client_id_back',2582617,'BIZ-B3F5E5D9'),
(182,'eff5214a-6a74-411b-8602-7a6d12695f27','guarantor_passport',2577879,'BIZ-B3F5E5D9'),
(183,'6bf30530-6685-4858-92e3-ca5ab40d5eb4','client_id_front',2576801,'BIZ-B3F5E5D9'),
(184,'2af09144-0b67-4b6d-9c62-c819cea266bf','guarantor_id_back',2575239,'BIZ-B3F5E5D9'),
(185,'e1304156-cc5e-4e0f-949c-4236e71103b6','guarantor_id_back',2569351,'BIZ-B3F5E5D9'),
(186,'a5973a7f-5b7e-4988-a324-620a513fc6df','guarantor_id_front',2569300,'BIZ-B3F5E5D9'),
(187,'b165758a-281e-4f8f-9dc2-cd477570e517','client_id_back',2567942,'BIZ-B3F5E5D9'),
(188,'15e160ef-8348-449f-a4b7-9fc165ec2fef','guarantor_id_back',2564638,'BIZ-B3F5E5D9'),
(189,'cf0247f2-8fe5-454a-bb47-845d4942132e','client_id_front',2554619,'BIZ-B3F5E5D9'),
(190,'ceecd077-b892-4d73-8810-a752b23f9fb4','guarantor_id_back',2554256,'BIZ-B3F5E5D9'),
(191,'d7400d15-91e8-4f19-a774-673a66fb212f','guarantor_id_front',2552191,'BIZ-B3F5E5D9'),
(192,'5171f4e1-060b-41c2-9e63-cf8c3d1249a2','client_id_back',2548838,'BIZ-B3F5E5D9'),
(193,'8d03a651-da5a-4eeb-a888-f95b0b54bc44','client_id_back',2544780,'BIZ-B3F5E5D9'),
(194,'24653084-be1b-4be5-8c52-371806e74f5e','client_id_front',2537441,'BIZ-B3F5E5D9'),
(195,'0b07753d-f483-4401-a499-7d067f8394c0','client_id_front',2528712,'BIZ-B3F5E5D9'),
(196,'fa247fdc-af3e-481d-aab4-3ad908c49c50','guarantor_id_back',2528692,'BIZ-B3F5E5D9'),
(197,'821697fa-a880-441b-bad2-6e2980d724f6','guarantor_id_back',2527626,'BIZ-B3F5E5D9'),
(198,'739253db-3ef9-41c5-af96-d36014a35e66','client_id_front',2511478,'BIZ-B3F5E5D9'),
(199,'01af9d38-66c3-4998-b712-b7b7da5d7e35','client_id_front',2508691,'BIZ-B3F5E5D9'),
(200,'9aff7563-152a-4b7a-8965-f1a333e8bf44','guarantor_id_front',2508691,'BIZ-B3F5E5D9')
ON CONFLICT (rank_no) DO NOTHING;
DO $$ BEGIN
  IF (SELECT count(*) FROM public.bripta_document_compression_manifest WHERE business_id='BIZ-B3F5E5D9')<>200 THEN
    RAISE EXCEPTION 'Manifest must contain exactly 200 Bripta records';
  END IF;
END $$;
DROP FUNCTION IF EXISTS public.bripta_document_compression_candidates(uuid,integer);
CREATE FUNCTION public.bripta_document_compression_candidates(
  p_object_id uuid DEFAULT NULL, p_limit integer DEFAULT 50, p_batch integer DEFAULT 1
)
RETURNS TABLE(rank_no integer,object_id uuid,client_id uuid,kind text,object_path text,original_bytes bigint)
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
  IF auth.uid() IS NULL OR NOT EXISTS (
    SELECT 1 FROM public.loan_staff s WHERE s.business_id='BIZ-B3F5E5D9'
      AND s.auth_user_id=auth.uid() AND coalesce(s.is_active,true)
      AND 'admin'=ANY(regexp_split_to_array(lower(trim(coalesce(s.role,''))),'\s*,\s*'))
  ) THEN RAISE EXCEPTION 'An active Bripta admin is required' USING ERRCODE='42501'; END IF;
  RETURN QUERY
  SELECT m.rank_no,o.id,d.client_id,d.kind,d.object_path,(o.metadata->>'size')::bigint
  FROM public.bripta_document_compression_manifest m
  JOIN storage.objects o ON o.id=m.object_id AND o.bucket_id='bripta-client-documents'
  JOIN public.bripta_client_documents d ON d.business_id='BIZ-B3F5E5D9' AND d.object_path=o.name
  WHERE m.business_id='BIZ-B3F5E5D9' AND d.kind=m.kind
    AND o.metadata->>'size' ~ '^[0-9]+$'
    AND (o.metadata->>'size')::bigint>1000000
    AND (o.metadata->>'size')::bigint=m.expected_bytes
    AND (p_object_id IS NULL OR m.object_id=p_object_id)
    AND (p_object_id IS NOT NULL OR m.rank_no BETWEEN (LEAST(GREATEST(p_batch,1),4)-1)*50+1 AND LEAST(GREATEST(p_batch,1),4)*50)
  ORDER BY m.rank_no LIMIT LEAST(GREATEST(coalesce(p_limit,50),1),50);
END;
$$;
REVOKE ALL ON FUNCTION public.bripta_document_compression_candidates(uuid,integer,integer) FROM PUBLIC,anon;
GRANT EXECUTE ON FUNCTION public.bripta_document_compression_candidates(uuid,integer,integer) TO authenticated;
CREATE TABLE IF NOT EXISTS public.bripta_document_compression_audit (
  object_id uuid PRIMARY KEY REFERENCES public.bripta_document_compression_manifest(object_id),
  client_id uuid NOT NULL,
  kind text NOT NULL,
  old_path text NOT NULL,
  new_path text NOT NULL,
  original_bytes bigint NOT NULL,
  compressed_bytes bigint NOT NULL,
  approved_by uuid NOT NULL,
  approval_note text NOT NULL,
  approved_at timestamptz NOT NULL DEFAULT now(),
  old_deleted_at timestamptz
);
ALTER TABLE public.bripta_document_compression_audit ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON public.bripta_document_compression_audit FROM PUBLIC,anon,authenticated;

CREATE OR REPLACE FUNCTION public.bripta_commit_document_compression(
  p_object_id uuid,p_new_path text,p_approval_note text
)
RETURNS TABLE(old_path text,new_path text,original_bytes bigint,compressed_bytes bigint)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_old storage.objects%rowtype; v_doc public.bripta_client_documents%rowtype;
  v_new_bytes bigint; v_rows integer;
BEGIN
  IF auth.uid() IS NULL OR NOT EXISTS (
    SELECT 1 FROM public.loan_staff s WHERE s.business_id='BIZ-B3F5E5D9'
      AND s.auth_user_id=auth.uid() AND coalesce(s.is_active,true)
      AND 'admin'=ANY(regexp_split_to_array(lower(trim(coalesce(s.role,''))),'\s*,\s*'))
  ) THEN RAISE EXCEPTION 'An active Bripta admin is required' USING ERRCODE='42501'; END IF;
  IF length(trim(coalesce(p_approval_note,'')))<8 THEN
    RAISE EXCEPTION 'Explicit compression approval is required'; END IF;
  SELECT o.* INTO v_old FROM storage.objects o
  JOIN public.bripta_document_compression_manifest m ON m.object_id=o.id
  WHERE o.id=p_object_id AND o.bucket_id='bripta-client-documents'
    AND m.business_id='BIZ-B3F5E5D9'
    AND o.metadata->>'size' ~ '^[0-9]+$'
    AND (o.metadata->>'size')::bigint=m.expected_bytes;
  IF v_old.id IS NULL OR v_old.metadata->>'size' !~ '^[0-9]+$'
    OR (v_old.metadata->>'size')::bigint<=1000000 THEN
    RAISE EXCEPTION 'Original is missing or has changed'; END IF;
  SELECT d.* INTO v_doc FROM public.bripta_client_documents d
  WHERE d.business_id='BIZ-B3F5E5D9' AND d.object_path=v_old.name FOR UPDATE;
  IF v_doc.client_id IS NULL OR p_new_path IS NULL
    OR p_new_path !~* ('^BIZ-B3F5E5D9/'||v_doc.client_id::text||'/'||v_doc.kind||'/[0-9a-f]{64}[.]jpg$')
    OR p_new_path=v_old.name THEN
    RAISE EXCEPTION 'Compressed file does not match the linked client document'; END IF;
  SELECT (o.metadata->>'size')::bigint INTO v_new_bytes FROM storage.objects o
  WHERE o.bucket_id='bripta-client-documents' AND o.name=p_new_path
    AND lower(o.metadata->>'mimetype')='image/jpeg'
    AND o.metadata->>'size' ~ '^[0-9]+$';
  IF v_new_bytes IS NULL OR v_new_bytes<10000 OR v_new_bytes>950000
    OR v_new_bytes>=(v_old.metadata->>'size')::bigint THEN
    RAISE EXCEPTION 'Compressed file is missing or does not meet the size limit'; END IF;
  UPDATE public.bripta_client_documents d
  SET object_path=p_new_path,file_size=v_new_bytes,mime_type='image/jpeg'
  WHERE d.client_id=v_doc.client_id AND d.kind=v_doc.kind AND d.object_path=v_old.name;
  GET DIAGNOSTICS v_rows=ROW_COUNT;
  IF v_rows<>1 THEN RAISE EXCEPTION 'Document link changed; retry review'; END IF;
  INSERT INTO public.bripta_document_compression_audit
    (object_id,client_id,kind,old_path,new_path,original_bytes,compressed_bytes,approved_by,approval_note)
  VALUES(p_object_id,v_doc.client_id,v_doc.kind,v_old.name,p_new_path,
    (v_old.metadata->>'size')::bigint,v_new_bytes,auth.uid(),trim(p_approval_note));
  RETURN QUERY SELECT v_old.name,p_new_path,(v_old.metadata->>'size')::bigint,v_new_bytes;
END;
$$;
REVOKE ALL ON FUNCTION public.bripta_commit_document_compression(uuid,text,text) FROM PUBLIC,anon;
GRANT EXECUTE ON FUNCTION public.bripta_commit_document_compression(uuid,text,text) TO authenticated;

CREATE OR REPLACE FUNCTION public.bripta_pending_old_document_cleanup(p_object_id uuid)
RETURNS text LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_path text;
BEGIN
  IF auth.uid() IS NULL OR NOT EXISTS (
    SELECT 1 FROM public.loan_staff s WHERE s.business_id='BIZ-B3F5E5D9'
      AND s.auth_user_id=auth.uid() AND coalesce(s.is_active,true)
      AND 'admin'=ANY(regexp_split_to_array(lower(trim(coalesce(s.role,''))),'\s*,\s*'))
  ) THEN RAISE EXCEPTION 'An active Bripta admin is required' USING ERRCODE='42501'; END IF;
  SELECT a.old_path INTO v_path FROM public.bripta_document_compression_audit a
  JOIN public.bripta_client_documents d ON d.client_id=a.client_id AND d.kind=a.kind
    AND d.business_id='BIZ-B3F5E5D9' AND d.object_path=a.new_path
  WHERE a.object_id=p_object_id AND a.old_deleted_at IS NULL;
  RETURN v_path;
END;
$$;
REVOKE ALL ON FUNCTION public.bripta_pending_old_document_cleanup(uuid) FROM PUBLIC,anon;
GRANT EXECUTE ON FUNCTION public.bripta_pending_old_document_cleanup(uuid) TO authenticated;

CREATE OR REPLACE FUNCTION public.bripta_mark_old_document_deleted(p_object_id uuid)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_old_path text;
BEGIN
  v_old_path:=public.bripta_pending_old_document_cleanup(p_object_id);
  IF v_old_path IS NULL THEN RETURN false; END IF;
  IF EXISTS(SELECT 1 FROM storage.objects o
    WHERE o.bucket_id='bripta-client-documents' AND o.name=v_old_path) THEN RETURN false; END IF;
  UPDATE public.bripta_document_compression_audit
  SET old_deleted_at=now() WHERE object_id=p_object_id AND old_deleted_at IS NULL;
  RETURN true;
END;
$$;
REVOKE ALL ON FUNCTION public.bripta_mark_old_document_deleted(uuid) FROM PUBLIC,anon;
GRANT EXECUTE ON FUNCTION public.bripta_mark_old_document_deleted(uuid) TO authenticated;
CREATE OR REPLACE FUNCTION public.bripta_pending_document_cleanup_ids(p_batch integer DEFAULT 1)
RETURNS TABLE(object_id uuid) LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
  IF auth.uid() IS NULL OR NOT EXISTS (
    SELECT 1 FROM public.loan_staff s WHERE s.business_id='BIZ-B3F5E5D9'
      AND s.auth_user_id=auth.uid() AND coalesce(s.is_active,true)
      AND 'admin'=ANY(regexp_split_to_array(lower(trim(coalesce(s.role,''))),'\s*,\s*'))
  ) THEN RAISE EXCEPTION 'An active Bripta admin is required' USING ERRCODE='42501'; END IF;
  RETURN QUERY SELECT a.object_id FROM public.bripta_document_compression_audit a
  JOIN public.bripta_document_compression_manifest m ON m.object_id=a.object_id
  WHERE a.old_deleted_at IS NULL
    AND m.rank_no BETWEEN (LEAST(GREATEST(p_batch,1),4)-1)*50+1 AND LEAST(GREATEST(p_batch,1),4)*50
  ORDER BY m.rank_no;
END;
$$;
REVOKE ALL ON FUNCTION public.bripta_pending_document_cleanup_ids(integer) FROM PUBLIC,anon;
GRANT EXECUTE ON FUNCTION public.bripta_pending_document_cleanup_ids(integer) TO authenticated;
COMMIT;
NOTIFY pgrst, 'reload schema';
